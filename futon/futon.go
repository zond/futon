package futon

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"code.google.com/p/goauth2/oauth"
	"google.golang.org/api/drive/v2"
)

// Settings for authorization.
var config = &oauth.Config{
	ClientId:     "944095575246-jq2jufr9k7s244jl9qb4nk1s36av4cd5.apps.googleusercontent.com",
	ClientSecret: "U0Okcw5_XHz8565QPRsi1Nun",
	Scope:        "https://www.googleapis.com/auth/drive",
	RedirectURL:  "urn:ietf:wg:oauth:2.0:oob",
	AuthURL:      "https://accounts.google.com/o/oauth2/auth",
	TokenURL:     "https://accounts.google.com/o/oauth2/token",
}

var uid uint32
var gid uint32

func init() {
	user, err := user.Current()
	if err == nil {
		uidint, _ := strconv.Atoi(user.Uid)
		gidint, _ := strconv.Atoi(user.Gid)
		uid = uint32(uidint)
		gid = uint32(gidint)
	}
}

type childFetch struct {
	file *drive.File
	err  error
}

// the common node functions
type node interface {
	fs.Node
	Path() path
	Futon() *Futon
	Id() string
	Size() int64
}

// the basic stuff all nodes have
type nodeImpl struct {
	futon *Futon
	path  path
	id    string
	mode  os.FileMode
	size  int64
	atime time.Time
	mtime time.Time
}

func (self *nodeImpl) Path() path {
	return self.path
}

func (self *nodeImpl) Id() string {
	return self.id
}

func (self *nodeImpl) Size() int64 {
	return self.size
}

func (self *nodeImpl) Attr() fuse.Attr {
	return fuse.Attr{
		Mode:  self.mode,
		Size:  uint64(self.size),
		Uid:   uid,
		Gid:   gid,
		Atime: self.atime,
		Mtime: self.mtime,
		Ctime: self.mtime,
	}
}

func (self *nodeImpl) Futon() *Futon {
	return self.futon
}

// since drive files can have / in them, i keep paths separated in a string slice
type path []string

// add returns a new path with the new element added at the end
func (self path) add(el string) (result path) {
	result = make(path, len(self)+1)
	copy(result, self)
	result[len(result)-1] = el
	return
}

// the string rep of a path replaces the / with the divisor sign (U+2215) to be able to return it as a file system path
func (self path) String() (result string) {
	for index, el := range self {
		result += strings.Replace(el, "/", "\u2215", -1)
		if index < len(self)-1 {
			result += "/"
		}
	}
	return
}

// base will return the file name (the last path element)
func (self path) base() string {
	return strings.Replace(self[len(self)-1], "/", "\u2215", -1)
}

// dir will return the parent dir of path (all but the last element)
func (self path) dir() (result string) {
	for index, el := range self[:len(self)-1] {
		result += strings.Replace(el, "/", "\u2215", -1)
		if index < len(self)-2 {
			result += "/"
		}
	}
	return
}

// folder is a node that has children cached
type folder struct {
	node
	children     []node          // used when listing children
	childByPath  map[string]node // used when looking up children by path
	childrenLock sync.RWMutex
}

func (self *folder) mergeChange(change *drive.Change) {
	childPath := self.Path().add(change.File.Title)
	if change.File.Labels.Trashed {
		if _, found := self.childByPath[childPath.String()]; found {
			for index, child := range self.children {
				if child.Id() == change.File.Id {
					self.childrenLock.Lock()
					newChildren := make([]node, len(self.children)-1)
					if index == 0 {
						copy(newChildren, self.children[1:])
					} else if index == len(self.children)-1 {
						copy(newChildren, self.children)
					} else {
						copy(newChildren, self.children[:index])
						copy(newChildren[index:], self.children[index+1:])
					}
					self.children = newChildren
					delete(self.childByPath, childPath.String())
					self.childrenLock.Unlock()
					break
				}
			}
		}
	} else {
		newChild := self.Futon().newNode(childPath, change.File)
		self.childrenLock.Lock()
		self.children = append(self.children, newChild)
		self.childByPath[childPath.String()] = newChild
		self.childrenLock.Unlock()
	}
}

func (self *folder) Lookup(name string, intr fs.Intr) (result fs.Node, ferr fuse.Error) {
	if err := self.loadChildren(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	childPath := self.Path().add(name)
	if n, found := self.childByPath[childPath.String()]; found {
		result = n
		return
	}
	ferr = fuse.Error(fmt.Errorf("No file %#v inside %#v", name, self.Path()))
	return
}

func (self *folder) clearChildren() {
	self.childrenLock.Lock()
	defer self.childrenLock.Unlock()
	self.children = nil
	self.childByPath = map[string]node{}
}

func (self *folder) loadChildren() (err error) {
	self.childrenLock.Lock()
	defer self.childrenLock.Unlock()
	if self.children == nil {
		var childLoader func(string) error
		foundChildren := 0
		childChan := make(chan childFetch)
		childLoader = func(pageToken string) (err error) {
			call := self.Futon().drive.Children.List(self.Id()).MaxResults(8192)
			if pageToken != "" {
				call = call.PageToken(pageToken)
			}
			var children *drive.ChildList
			if children, err = call.Do(); err != nil {
				self.Futon().log("While trying to load children for %#v: %v", self.Path().String(), err)
				return
			}
			for _, child := range children.Items {
				childCopy := child
				foundChildren++
				go func() {
					fetch := childFetch{}
					fetch.file, fetch.err = self.Futon().drive.Files.Get(childCopy.Id).Do()
					childChan <- fetch
				}()
			}
			if children.NextPageToken != "" {
				if err = childLoader(children.NextPageToken); err != nil {
					return
				}
			}
			return
		}
		if err = childLoader(""); err != nil {
			return
		}
		for i := 0; i < foundChildren; i++ {
			child := <-childChan
			if child.err != nil {
				return
			}
			if !child.file.Labels.Trashed {
				childPath := self.Path().add(child.file.Title)
				n := self.Futon().newNode(childPath, child.file)
				self.children = append(self.children, n)
				self.childByPath[n.Path().String()] = n
			}
		}
	}
	return
}

func (self *folder) ReadDir(intr fs.Intr) (result []fuse.Dirent, ferr fuse.Error) {
	if err := self.Futon().cleanChanges(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	if err := self.loadChildren(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	result = make([]fuse.Dirent, len(self.children))
	self.childrenLock.RLock()
	defer self.childrenLock.RUnlock()
	for index, child := range self.children {
		ent := fuse.Dirent{
			Name: child.Path().base(),
		}
		switch child.(type) {
		case *folder:
			ent.Type = fuse.DT_Dir
		case *file:
			ent.Type = fuse.DT_File
		default:
			ferr = fuse.Error(fmt.Errorf("Unknown node implementation: %#v", child))
			return
		}
		result[index] = ent
	}
	return
}

// file is a node that has a download url
type file struct {
	node
	downloadUrl string
}

func (self *file) getDownloadUrl() (result string, err error) {
	if self.downloadUrl == "" {
		var f *drive.File
		if f, err = self.Futon().drive.Files.Get(self.Id()).Do(); err != nil {
			self.Futon().log("While trying to get %#v: %v", self.Path().String(), err)
			return
		}
		self.downloadUrl = f.DownloadUrl
	}
	result = self.downloadUrl
	return
}

func (self *file) read(readReq *fuse.ReadRequest, readResp *fuse.ReadResponse, tries int) (ferr fuse.Error) {
	if self.Size() == 0 {
		return
	}
	downloadUrl, err := self.getDownloadUrl()
	if err != nil {
		return
	}
	if downloadUrl == "" {
		return
	}
	req, err := http.NewRequest("GET", downloadUrl, nil)
	if err != nil {
		self.Futon().log("While trying to create GET request for %#v: %v", downloadUrl, err)
		ferr = fuse.Error(err)
		return
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", readReq.Offset, readReq.Offset+int64(readReq.Size)-1))
	resp, err := self.Futon().client.Do(req)
	if err != nil {
		self.Futon().log("While trying to perform %+v: %v", req, err)
		ferr = fuse.Error(err)
		return
	}
	if resp.StatusCode > 400 && resp.StatusCode < 404 {
		self.downloadUrl = ""
		if tries < 5 {
			return self.read(readReq, readResp, tries+1)
		}
	}
	if resp.StatusCode != 206 {
		log.Printf("%+v", resp)
		ferr = fuse.Error(fmt.Errorf("While trying to download %#v: %+v", downloadUrl, resp))
		return
	}
	defer resp.Body.Close()
	if readResp.Data, err = ioutil.ReadAll(resp.Body); err != nil {
		self.Futon().log("While trying to read the body of %+v: %v", resp, err)
		ferr = fuse.Error(err)
		return
	}
	return
}

func (self *file) Read(readReq *fuse.ReadRequest, readResp *fuse.ReadResponse, intr fs.Intr) (ferr fuse.Error) {
	return self.read(readReq, readResp, 0)
}

type Futon struct {
	dir          string
	authorizer   func(string) string
	drive        *drive.Service
	mountpoint   string
	rootFolderId string
	lastChange   int64
	unmounted    int32
	conn         *fuse.Conn
	client       *http.Client
	logger       *log.Logger

	folderCacheById   map[string]*folder // used when cleaning up folders that are subjected to change
	folderCacheByPath map[string]*folder // used when finding folders by path
	folderCacheLock   sync.RWMutex

	Token *oauth.Token
}

func (self *Futon) changeList() (result []*drive.Change, err error) {
	var changeLoader func(string) error
	newLastChange := int64(0)
	changeLoader = func(pageToken string) (err error) {
		call := self.drive.Changes.List().StartChangeId(self.lastChange).IncludeDeleted(true)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		changes, err := call.Do()
		if err != nil {
			self.log("While trying to get changes: %v", err)
			return
		}
		for _, change := range changes.Items {
			changeCopy := change
			newLastChange = changeCopy.Id
			if changeCopy.Id != self.lastChange {
				result = append(result, changeCopy)
			}
		}
		if changes.NextPageToken != "" {
			if err = changeLoader(changes.NextPageToken); err != nil {
				return
			}
		}
		return
	}
	if err = changeLoader(""); err != nil {
		return
	}
	self.lastChange = newLastChange
	return
}

func (self *Futon) cleanChanges() (err error) {
	changes, err := self.changeList()
	if err != nil {
		return
	}
	self.folderCacheLock.Lock()
	defer self.folderCacheLock.Unlock()
	for _, change := range changes {
		for _, parent := range change.File.Parents {
			if d, found := self.folderCacheById[parent.Id]; found {
				d.mergeChange(change)
			}
		}
	}
	return
}

func (self *Futon) acquireToken() (err error) {
	authUrl := config.AuthCodeURL("state")
	code := self.authorizer(authUrl)
	t := &oauth.Transport{
		Config:    config,
		Transport: http.DefaultTransport,
	}
	if self.Token, err = t.Exchange(code); err != nil {
		self.log("While trying to exchange %#v for a refresh token: %v", code, err)
		return
	}
	return
}

func (self *Futon) authorize() (err error) {
	if err = os.MkdirAll(filepath.Dir(self.dir), 0700); err != nil {
		return
	}
	configPath := filepath.Join(self.dir, "config")
	f, err := os.Open(configPath)
	if err != nil {
		if !os.IsNotExist(err) {
			self.log("While trying to open %#v: %v", configPath, err)
			return
		}
		if err = self.acquireToken(); err != nil {
			return
		}
		if f, err = os.Create(configPath); err != nil {
			self.log("While trying to create %#v: %v", configPath, err)
			return
		}
		defer f.Close()
		if err = json.NewEncoder(f).Encode(self); err != nil {
			self.log("While trying to JSON encode %+v: %v", self, err)
			return
		}
	} else {
		defer f.Close()
		if err = json.NewDecoder(f).Decode(self); err != nil {
			self.log("While trying to JSON decode %#v: %v", configPath, err)
			return
		}
	}
	transport := &oauth.Transport{
		Config:    config,
		Token:     self.Token,
		Transport: http.DefaultTransport,
	}
	self.client = transport.Client()
	if self.drive, err = drive.New(self.client); err != nil {
		return
	}

	about, err := self.drive.About.Get().Do()
	if err != nil {
		self.log("While trying to get About: %v", err)
		return
	}
	self.rootFolderId = about.RootFolderId
	self.lastChange = about.LargestChangeId

	self.log("Connected to Google Drive")

	return
}

func (self *Futon) newNode(path path, f *drive.File) (result node) {
	atime, _ := time.Parse(time.RFC3339, f.LastViewedByMeDate)
	mtime, _ := time.Parse(time.RFC3339, f.ModifiedDate)
	base := &nodeImpl{
		futon: self,
		path:  path,
		id:    f.Id,
		mode:  os.FileMode(0400),
		size:  f.FileSize,
		atime: atime,
		mtime: mtime,
	}
	if f.MimeType == "application/vnd.google-apps.folder" {
		base.mode |= 0100
		base.mode |= os.ModeDir
		d := &folder{
			childByPath: map[string]node{},
			node:        base,
		}
		self.folderCacheLock.Lock()
		defer self.folderCacheLock.Unlock()
		self.folderCacheByPath[base.Path().String()] = d
		self.folderCacheById[base.Id()] = d
		result = d
	} else {
		result = &file{
			node: base,
		}
	}
	return
}

func (self *Futon) lookup(path path) (result fs.Node, ferr fuse.Error) {
	self.folderCacheLock.RLock()
	d, found := self.folderCacheByPath[path.String()]
	self.folderCacheLock.RUnlock()
	if found {
		result = d
		return
	}
	if len(path) == 0 {
		result = self.newNode(path, &drive.File{
			Id: self.rootFolderId,
			UserPermission: &drive.Permission{
				Type: "owner",
			},
			MimeType: "application/vnd.google-apps.folder",
		})
		return
	}
	parent, err := self.lookup(path[:len(path)-1])
	if err != nil {
		return
	}
	if d, ok := parent.(*folder); ok {
		return d.Lookup(path[len(path)-1], make(fs.Intr))
	}
	ferr = fuse.Error(fmt.Errorf("No file %#v found"))
	return
}

func (self *Futon) Root() (result fs.Node, err fuse.Error) {
	return self.lookup(nil)
}

func (self *Futon) log(format string, params ...interface{}) {
	if self.logger != nil {
		self.logger.Printf(fmt.Sprintf("%v\t%v\t%v", "futon", time.Now(), format), params...)
	}
}

func (self *Futon) Logger(l *log.Logger) *Futon {
	self.logger = l
	return self
}

func (self *Futon) unmount() {
	if atomic.CompareAndSwapInt32(&self.unmounted, 0, 1) {
		if err := fuse.Unmount(self.mountpoint); err != nil {
			self.log("While trying to unmount %#v: %v", self.mountpoint, err)
		}
		if err := self.conn.Close(); err != nil {
			self.log("While trying to close %#v: %v", self.mountpoint, err)
		}
	}
	self.log("Unmounted %#v", self.mountpoint)
}

func (self *Futon) Mount() (err error) {
	if err = self.authorize(); err != nil {
		return
	}

	if self.conn, err = fuse.Mount(
		self.mountpoint,
		fuse.FSName("futon"),
		fuse.Subtype("futon"),
		fuse.LocalVolume(),
		fuse.VolumeName("futon"),
	); err != nil {
		self.log("While trying to mount %#v: %v", self.mountpoint, err)
		return
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt)
	go func() {
		for _ = range interruptChan {
			self.unmount()
		}
	}()
	defer self.unmount()

	self.log("Mounted %#v", self.mountpoint)
	if err = fs.Serve(self.conn, self); err != nil {
		self.log("While trying to serve %#v: %v", self.mountpoint, err)
		return
	}

	<-self.conn.Ready
	if err = self.conn.MountError; err != nil {
		self.log("While serving %#v: %v", self.mountpoint, err)
		return
	}

	return
}

/*
New will return a new Futon with the provided mountpoint, dir and authorization callback
*/
func New(mountpoint, dir string, authorizer func(url string) (code string)) (result *Futon) {
	result = &Futon{
		folderCacheById:   map[string]*folder{},
		folderCacheByPath: map[string]*folder{},
		mountpoint:        mountpoint,
		dir:               dir,
		authorizer:        authorizer,
	}
	return
}

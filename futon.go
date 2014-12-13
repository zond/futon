package main

import (
	"encoding/json"
	"flag"
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

	"google.golang.org/api/drive/v2"

	"code.google.com/p/goauth2/oauth"
)

type futon struct {
	auth              string
	account           string
	drive             *drive.Service
	mountpoint        string
	rootFolderId      string
	lastChange        int64
	folderCacheById   map[string]*dir
	folderCacheByPath map[string]*dir
	folderCacheLock   sync.RWMutex
	unmounted         int32
	conn              *fuse.Conn
	client            *http.Client

	Token *oauth.Token
}

// Settings for authorization.
var config = &oauth.Config{
	ClientId:     "944095575246-jq2jufr9k7s244jl9qb4nk1s36av4cd5.apps.googleusercontent.com",
	ClientSecret: "U0Okcw5_XHz8565QPRsi1Nun",
	Scope:        "https://www.googleapis.com/auth/drive",
	RedirectURL:  "urn:ietf:wg:oauth:2.0:oob",
	AuthURL:      "https://accounts.google.com/o/oauth2/auth",
	TokenURL:     "https://accounts.google.com/o/oauth2/token",
}

func (self *futon) changeList() (result []*drive.Change, err error) {
	var changeLoader func(string) error
	newLastChange := int64(0)
	changeLoader = func(pageToken string) (err error) {
		call := self.drive.Changes.List().StartChangeId(self.lastChange).IncludeDeleted(true)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		changes, err := call.Do()
		if err != nil {
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

func (self *futon) cleanChanges() (err error) {
	changes, err := self.changeList()
	if err != nil {
		return
	}
	self.folderCacheLock.Lock()
	defer self.folderCacheLock.Unlock()
	for _, change := range changes {
		for _, parent := range change.File.Parents {
			if d, found := self.folderCacheById[parent.Id]; found {
				fmt.Println("cleaning", d.Path(), "since", change.File.Title, "was changed")
				d.clearChildren()
			}
		}
	}
	return
}

func (self *futon) acquireToken() (err error) {
	authUrl := config.AuthCodeURL("state")
	fmt.Printf("Go to the following link in your browser: %v\n", authUrl)
	t := &oauth.Transport{
		Config:    config,
		Transport: http.DefaultTransport,
	}
	fmt.Printf("Enter verification code: ")
	var code string
	fmt.Scanln(&code)
	if self.Token, err = t.Exchange(code); err != nil {
		return
	}
	return
}

func (self *futon) authorize() (err error) {
	if err = os.MkdirAll(filepath.Dir(self.auth), 0700); err != nil {
		return
	}
	f, err := os.Open(self.auth)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
		if err = self.acquireToken(); err != nil {
			return
		}
		if f, err = os.Create(self.auth); err != nil {
			return
		}
		defer f.Close()
		if err = json.NewEncoder(f).Encode(self); err != nil {
			return
		}
	} else {
		defer f.Close()
		if err = json.NewDecoder(f).Decode(self); err != nil {
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
		return
	}
	self.rootFolderId = about.RootFolderId
	self.lastChange = about.LargestChangeId

	return
}

type nodeImpl struct {
	futon *futon
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

type node interface {
	fs.Node
	Path() path
	Futon() *futon
	Id() string
	Size() int64
}

func (self *nodeImpl) Id() string {
	return self.id
}

func (self *nodeImpl) Size() int64 {
	return self.size
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

type path []string

func (self path) add(el string) (result path) {
	result = make(path, len(self)+1)
	copy(result, self)
	result[len(result)-1] = el
	return
}

func (self path) String() (result string) {
	for index, el := range self {
		result += strings.Replace(el, "/", "\u2215", -1)
		if index < len(self)-1 {
			result += "/"
		}
	}
	return
}

func (self path) base() string {
	return strings.Replace(self[len(self)-1], "/", "\u2215", -1)
}

func (self path) dir() (result string) {
	for index, el := range self[:len(self)-1] {
		result += strings.Replace(el, "/", "\u2215", -1)
		if index < len(self)-2 {
			result += "/"
		}
	}
	return
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

func (self *nodeImpl) Futon() *futon {
	return self.futon
}

type dir struct {
	node
	children     []node
	childByName  map[string]node
	childrenLock sync.RWMutex
}

type file struct {
	node
	downloadUrl string
}

func (self *file) getDownloadUrl() (result string, err error) {
	if self.downloadUrl == "" {
		var f *drive.File
		if f, err = self.Futon().drive.Files.Get(self.Id()).Do(); err != nil {
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
		log.Printf("%v", err)
		ferr = fuse.Error(err)
		return
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", readReq.Offset, readReq.Offset+int64(readReq.Size)-1))
	resp, err := self.Futon().client.Do(req)
	if err != nil {
		log.Printf("%v", err)
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
		log.Printf("%v", err)
		ferr = fuse.Error(err)
		return
	}
	return
}

func (self *file) Read(readReq *fuse.ReadRequest, readResp *fuse.ReadResponse, intr fs.Intr) (ferr fuse.Error) {
	return self.read(readReq, readResp, 0)
}

func (self *futon) newNode(path path, f *drive.File) (result node) {
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
	if f.UserPermission.Type == "owner" || f.UserPermission.Type == "writer" {
		base.mode |= 0200
	}
	if f.MimeType == "application/vnd.google-apps.folder" {
		base.mode |= 0100
		base.mode |= os.ModeDir
		d := &dir{
			childByName: map[string]node{},
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

type childFetch struct {
	file *drive.File
	err  error
}

func (self *dir) Lookup(name string, intr fs.Intr) (result fs.Node, ferr fuse.Error) {
	if err := self.loadChildren(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	childPath := self.Path().add(name)
	if n, found := self.childByName[childPath.String()]; found {
		result = n
		return
	}
	ferr = fuse.Error(fmt.Errorf("No file %#v inside %#v", name, self.Path()))
	return
}

func (self *futon) lookup(path path) (result fs.Node, ferr fuse.Error) {
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
	if d, ok := parent.(*dir); ok {
		return d.Lookup(path[len(path)-1], make(fs.Intr))
	}
	ferr = fuse.Error(fmt.Errorf("No file %#v found"))
	return
}

func (self *dir) clearChildren() {
	self.childrenLock.Lock()
	defer self.childrenLock.Unlock()
	self.children = nil
	self.childByName = map[string]node{}
}

func (self *dir) loadChildren() (err error) {
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
				self.childByName[n.Path().String()] = n
			}
		}
	}
	return
}

func (self *dir) ReadDir(intr fs.Intr) (result []fuse.Dirent, ferr fuse.Error) {
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
		case *dir:
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

func (self *futon) Root() (result fs.Node, err fuse.Error) {
	return self.lookup(nil)
}

func (self *futon) unmount() {
	if atomic.CompareAndSwapInt32(&self.unmounted, 0, 1) {
		if err := fuse.Unmount(self.mountpoint); err != nil {
			log.Printf("Trying to unmount %#v: %v", self.mountpoint, err)
		}
		if err := self.conn.Close(); err != nil {
			log.Printf("Trying to close %#v: %v", self.mountpoint, err)
		}
	}
	log.Printf("Unmounted %#v", self.mountpoint)
}

func (self *futon) serve() (err error) {
	if self.conn, err = fuse.Mount(
		self.mountpoint,
		fuse.FSName("futon"),
		fuse.Subtype("Google Drive"),
		fuse.LocalVolume(),
		fuse.VolumeName(self.account),
	); err != nil {
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

	log.Printf("Mounted %#v", self.mountpoint)
	if err = fs.Serve(self.conn, self); err != nil {
		return
	}

	<-self.conn.Ready
	if err = self.conn.MountError; err != nil {
		return
	}

	return
}

func main() {
	auth := flag.String("auth", filepath.Join(os.Getenv("HOME"), ".futon", "auth"), "Which file should contain the auth token")
	account := flag.String("account", "", "Which Google Account to use when connecting to Drive to fetch the first auth token")

	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	mountpoint := flag.Arg(0)

	f := &futon{
		folderCacheById:   map[string]*dir{},
		folderCacheByPath: map[string]*dir{},
		mountpoint:        mountpoint,
		auth:              *auth,
		account:           *account,
	}

	log.Printf("Connecting to Google Drive")
	if err := f.authorize(); err != nil {
		panic(err)
	}

	if err := f.serve(); err != nil {
		panic(err)
	}

}

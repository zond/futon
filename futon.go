package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
	changeLoader = func(pageToken string) (err error) {
		call := self.drive.Changes.List().StartChangeId(self.lastChange)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		changes, err := call.Do()
		if err != nil {
			return
		}
		for _, change := range changes.Items {
			changeCopy := change
			self.lastChange = changeCopy.Id
			result = append(result, changeCopy)
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
	return
}

func (self *futon) cleanChanges() (err error) {
	start := time.Now()
	changes, err := self.changeList()
	if err != nil {
		return
	}
	for _, change := range changes {
		log.Printf("%+v\n", change)
	}
	log.Printf("changes: %v", time.Since(start))
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
	if self.drive, err = drive.New(transport.Client()); err != nil {
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
	path  string
	id    string
	mode  os.FileMode
}

func (self *nodeImpl) Path() string {
	return self.path
}

type node interface {
	fs.Node
	Path() string
	Futon() *futon
	Id() string
}

func (self *nodeImpl) Id() string {
	return self.id
}

func (self *nodeImpl) Attr() fuse.Attr {
	return fuse.Attr{Mode: self.mode}
}

func (self *nodeImpl) Futon() *futon {
	return self.futon
}

type dir struct {
	node
	children    []node
	childByName map[string]node
}

func (self *futon) newDir(path, id string, mode os.FileMode) (result *dir) {
	result = &dir{
		childByName: map[string]node{},
		node: &nodeImpl{
			futon: self,
			path:  path,
			id:    id,
			mode:  os.ModeDir | 0777,
		},
	}
	self.folderCacheLock.Lock()
	defer self.folderCacheLock.Unlock()
	self.folderCacheByPath[path] = result
	self.folderCacheById[result.Id()] = result
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
	if n, found := self.childByName[filepath.Join(self.Path(), name)]; found {
		result = n
		return
	}
	ferr = fuse.Error(fmt.Errorf("No file %#v inside %#v", name, self.Path()))
	return
}

func (self *futon) lookup(path string) (result fs.Node, ferr fuse.Error) {
	self.folderCacheLock.RLock()
	d, found := self.folderCacheByPath[path]
	self.folderCacheLock.RUnlock()
	if found {
		result = d
		return
	}
	if path == "/" {
		result = self.newDir(path, self.rootFolderId, 0777)
		return
	}
	parent, err := self.lookup(filepath.Dir(path))
	if err != nil {
		return
	}
	if d, ok := parent.(*dir); ok {
		return d.Lookup(filepath.Base(path), make(fs.Intr))
	}
	ferr = fuse.Error(fmt.Errorf("No file %#v found"))
	return
}

func (self *dir) loadChildren() (err error) {
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

			var n node
			if child.file.MimeType == "application/vnd.google-apps.folder" {
				n = self.Futon().newDir(filepath.Join(self.Path(), child.file.Title), child.file.Id, 0777)
			} else {
				n = self.Futon().newFile(filepath.Join(self.Path(), child.file.Title), child.file.Id, 0777)
			}
			self.children = append(self.children, n)
			self.childByName[n.Path()] = n
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
	for index, child := range self.children {
		ent := fuse.Dirent{
			Name: strings.Replace(filepath.Base(child.Path()), "/", "\u2215", -1),
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

type file struct {
	node
}

func (self *futon) newFile(path, id string, mode os.FileMode) *file {
	return &file{
		node: &nodeImpl{
			futon: self,
			path:  path,
			id:    id,
			mode:  mode,
		},
	}
}

func (self *futon) Root() (result fs.Node, err fuse.Error) {
	return self.lookup("/")
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

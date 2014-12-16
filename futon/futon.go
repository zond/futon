package futon

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"bytes"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"code.google.com/p/goauth2/oauth"
	"github.com/boltdb/bolt"
	"google.golang.org/api/drive/v2"
)

var nodeByPath = []byte("nodeByPath")
var nodePathById = []byte("nodePathById")
var childPathsByPath = []byte("childPathsByPath")
var blocksByPath = []byte("blocksByPath")

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

type path []string

func (self path) add(el string) (result path) {
	result = make(path, len(self)+1)
	copy(result, self)
	result[len(result)-1] = el
	return
}

func (self path) String() (result string) {
	result = "/"
	for index, el := range self {
		result += strings.Replace(el, "/", "\u2215", -1)
		if index < len(self)-1 {
			result += "/"
		}
	}
	return
}

func (self path) name() string {
	if len(self) == 0 {
		return ""
	}
	return strings.Replace(self[len(self)-1], "/", "\u2215", -1)
}

func (self path) parent() path {
	return self[:len(self)-1]
}

func (self path) marshal() []byte {
	return []byte(self.String())
}

func (self *path) unmarshal(b []byte) {
	if bytes.Compare(b, []byte("/")) == 0 {
		return
	}
	*self = nil
	for _, el := range strings.Split(string(b), "/")[1:] {
		*self = append(*self, strings.Replace(el, "\u2215", "/", -1))
	}
}

type node struct {
	futon       *Futon
	downloadUrl string

	LoadedChildren bool
	Path           path
	Id             string
	Mode           os.FileMode
	Size           int64
	Atime          time.Time
	Mtime          time.Time
}

func (self *node) Attr() fuse.Attr {
	return fuse.Attr{
		Mode:  self.Mode,
		Size:  uint64(self.Size),
		Uid:   uid,
		Gid:   gid,
		Atime: self.Atime,
		Mtime: self.Mtime,
		Ctime: self.Mtime,
	}
}

func stack() string {
	buf := make([]byte, 4096)
	runtime.Stack(buf, false)
	return strings.Join(strings.Split(string(buf), "\n")[3:14], "\n")
}

func (self *node) rawSave(tx *bolt.Tx) (err error) {
	bucket, err := tx.CreateBucketIfNotExists(nodeByPath)
	if err != nil {
		self.futon.fatal("While trying to create nodeByPath bucket: %v", err)
		return
	}
	data, err := json.Marshal(self)
	if err != nil {
		self.futon.fatal("While trying to JSON encode %+v: %v", self, err)
		return
	}
	if err = bucket.Put(self.Path.marshal(), data); err != nil {
		self.futon.fatal("While trying to save %+v: %v", self, err)
		return
	}
	if bucket, err = tx.CreateBucketIfNotExists(nodePathById); err != nil {
		self.futon.fatal("While trying to create nodePathById bucket: %v", err)
		return
	}
	if err = bucket.Put([]byte(self.Id), self.Path.marshal()); err != nil {
		self.futon.fatal("While trying to save %#v: %v", self.Path, err)
		return
	}
	if len(self.Path) > 0 {
		if bucket, err = tx.CreateBucketIfNotExists(childPathsByPath); err != nil {
			self.futon.fatal("While trying to create childPathsByPath bucket: %v", err)
			return
		}
		if bucket, err = bucket.CreateBucketIfNotExists(self.Path.parent().marshal()); err != nil {
			self.futon.fatal("While trying to create child bucket for %+v: %v", self, err)
			return
		}
		if err = bucket.Put(self.Path.marshal(), self.Path.marshal()); err != nil {
			self.futon.fatal("While trying to save %#v: %v", self.Path, err)
			return
		}
	}
	return
}

func (self *node) save() (err error) {
	if err = self.futon.db.Update(self.rawSave); err != nil {
		return
	}
	return
}

func (self *node) createChildren() (err error) {
	if !self.LoadedChildren {
		self.LoadedChildren = true
		var childLoader func(string) error
		childLoader = func(pageToken string) (err error) {
			call := self.futon.drive.Files.List().Q(fmt.Sprintf("'%s' in parents and trashed = false", self.Id)).MaxResults(8192)
			if pageToken != "" {
				call = call.PageToken(pageToken)
			}
			var children *drive.FileList
			self.futon.debug("Listing children of %v", self.Path)
			if children, err = call.Do(); err != nil {
				self.futon.fatal("While trying to load children for %#v: %v", self.Path.String(), err)
				return
			}
			self.futon.debug("Done listing children of %v", self.Path)
			for _, child := range children.Items {
				if err = self.futon.newNode(self.Path.add(child.Title), child).save(); err != nil {
					return
				}
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
		if err = self.save(); err != nil {
			return
		}
	}
	return
}

func (self *node) Lookup(name string, intr fs.Intr) (result fs.Node, ferr fuse.Error) {
	if err := self.futon.mergeChanges(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	if err := self.createChildren(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	if err := self.futon.db.View(func(tx *bolt.Tx) (err error) {
		if bucket := tx.Bucket(nodeByPath); bucket != nil {
			if data := bucket.Get(self.Path.add(name).marshal()); data != nil {
				n := &node{
					futon: self.futon,
				}
				if err = json.Unmarshal(data, n); err != nil {
					self.futon.fatal("While trying to JSON decode %s: %v", data, err)
					return
				}
				result = n
			}
		}
		return
	}); err != nil {
		ferr = fuse.Error(err)
		return
	}
	if result == nil {
		ferr = fuse.Error(fmt.Errorf("%#v not found in %#v", name, self.Path.String()))
		return
	}
	return
}

func (self *node) ReadDir(intr fs.Intr) (result []fuse.Dirent, ferr fuse.Error) {
	if err := self.futon.mergeChanges(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	if err := self.createChildren(); err != nil {
		ferr = fuse.Error(err)
		return
	}
	children, err := self.futon.children(self.Path)
	if err != nil {
		ferr = fuse.Error(err)
		return
	}
	result = make([]fuse.Dirent, len(children))
	for index, child := range children {
		ent := fuse.Dirent{
			Name: child.Path.name(),
		}
		if child.Mode&os.ModeDir == 0 {
			ent.Type = fuse.DT_File
		} else {
			ent.Type = fuse.DT_Dir
		}
		result[index] = ent
	}
	return
}

func (self *node) getDownloadUrl() (result string, err error) {
	if self.downloadUrl == "" {
		var f *drive.File
		self.futon.debug("Getting %v", self.Path)
		if f, err = self.futon.drive.Files.Get(self.Id).Do(); err != nil {
			self.futon.fatal("While trying to get %#v: %v", self.Path.String(), err)
			return
		}
		self.futon.debug("Done getting %v", self.Path)
		self.downloadUrl = f.DownloadUrl
	}
	result = self.downloadUrl
	return
}

const (
	blockSize = 1 << 19
)

func (self *node) read(readReq *fuse.ReadRequest, readResp *fuse.ReadResponse) (err error) {
	if self.Size == 0 {
		return
	}
	readResp.Data = make([]byte, readReq.Size)
	firstBlock := readReq.Offset / blockSize
	lastBlock := (readReq.Offset + int64(readReq.Size)) / blockSize
	blockOffset := readReq.Offset % blockSize
	self.futon.debug("Loading %v bytes from %v of %v entails loading blocks from %v to %v", readReq.Size, readReq.Offset, self.Path, firstBlock, lastBlock)
	var block []byte
	copied := 0
	for i := firstBlock; i <= lastBlock; i++ {
		if block, err = self.loadBlock(i, 0); err != nil {
			return
		}
		dst := readResp.Data[copied:]
		if len(block) < int(blockOffset) {
			self.futon.fatal("block %v of %v is %v long, but our blockOffset is %v", i, self.Path, len(block), blockOffset)
		}
		src := block[blockOffset:]
		copy(dst, src)
		if len(dst) > len(src) {
			copied += len(src)
		} else {
			copied += len(dst)
		}
		blockOffset = 0
	}
	return
}

func (self *node) blockKey(n int64) (result []byte) {
	result = make([]byte, binary.MaxVarintLen64)
	result = result[:binary.PutVarint(result, n)]
	return
}

func (self *node) loadBlock(n int64, tries int) (result []byte, err error) {
	if err = self.futon.db.View(func(tx *bolt.Tx) (err error) {
		if blocksByPath := tx.Bucket(blocksByPath); blocksByPath != nil {
			if myBlocks := blocksByPath.Bucket([]byte(self.Id)); myBlocks != nil {
				block := myBlocks.Get(self.blockKey(n))
				if block != nil {
					result = make([]byte, len(block))
					copy(result, block)
				}
			}
		}
		return
	}); err != nil || result != nil {
		if result != nil {
			self.futon.debug("Returning %v bytes (block size %v) of block %v of %v after loading from bolt", len(result), blockSize, n, self.Path)
		}
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
		self.futon.fatal("While trying to create GET request for %#v: %v", downloadUrl, err)
		return
	}
	end := blockSize*(n+1) - 1
	if end > self.Size-1 {
		end = self.Size - 1
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", blockSize*n, end))
	self.futon.debug("Loading bytes %v-%v of %v", blockSize*n, end, self.Path)
	resp, err := self.futon.client.Do(req)
	if err != nil {
		self.futon.fatal("While trying to perform %+v: %v", req, err)
		return
	}
	self.futon.debug("Done loading bytes %v-%v of %v", blockSize*n, end, self.Path)
	if resp.StatusCode > 400 && resp.StatusCode < 404 {
		self.downloadUrl = ""
		if tries < 5 {
			return self.loadBlock(n, tries+1)
		}
	}
	if resp.StatusCode != 206 {
		log.Printf("%+v", resp)
		err = fmt.Errorf("While trying to download %#v: %+v", downloadUrl, resp)
		return
	}
	defer resp.Body.Close()
	if result, err = ioutil.ReadAll(resp.Body); err != nil {
		self.futon.fatal("While trying to read the body of %+v: %v", resp, err)
		return
	}
	if err = self.futon.db.Update(func(tx *bolt.Tx) (err error) {
		blocksByPath, err := tx.CreateBucketIfNotExists(blocksByPath)
		if err != nil {
			self.futon.fatal("While trying to create blocksByPath bucket: %v", err)
			return
		}
		myBlocks, err := blocksByPath.CreateBucketIfNotExists([]byte(self.Id))
		if err != nil {
			self.futon.fatal("While trying to create myBlocks bucket: %v", err)
			return
		}
		if err = myBlocks.Put(self.blockKey(n), result); err != nil {
			self.futon.fatal("While trying to store block %v of %v: %v", n, self.Path, err)
			return
		}
		self.futon.debug("Stored block %v of %v in bolt", n, self.Path)
		return
	}); err != nil {
		return
	}
	self.futon.debug("Returning %v bytes (block size %v) of block %v of %v after loading from Drive", len(result), blockSize, n, self.Path)
	return
}

func (self *node) Read(readReq *fuse.ReadRequest, readResp *fuse.ReadResponse, intr fs.Intr) (ferr fuse.Error) {
	if err := self.read(readReq, readResp); err != nil {
		ferr = fuse.Error(err)
		return
	}
	return
}

type Futon struct {
	dir             string
	db              *bolt.DB
	authorizer      func(string) string
	drive           *drive.Service
	mountpoint      string
	unmounted       int32
	conn            *fuse.Conn
	client          *http.Client
	logger          *log.Logger
	loglevel        int
	lastChangeCheck int64

	RootFolderId string
	LastChange   int64
	Token        *oauth.Token
}

func (self *Futon) saveMetadata() (err error) {
	if err = self.db.Update(func(tx *bolt.Tx) (err error) {
		metaData, err := json.Marshal(self)
		if err != nil {
			self.fatal("While trying to JSON encode %+v: %v", self, err)
			return
		}
		bucket, err := tx.CreateBucketIfNotExists(meta)
		if err != nil {
			self.fatal("While trying to create meta bucket: %v", err)
			return
		}
		if err = bucket.Put(meta, metaData); err != nil {
			self.fatal("While trying to save %+v: %v", self, err)
			return
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *Futon) changeList() (result []*drive.Change, err error) {
	var changeLoader func(string) error
	newLastChange := int64(0)
	changeLoader = func(pageToken string) (err error) {
		call := self.drive.Changes.List().StartChangeId(self.LastChange).IncludeDeleted(true)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		self.debug("Listing changes")
		changes, err := call.Do()
		if err != nil {
			self.fatal("While trying to get changes: %v", err)
			return
		}
		self.debug("Done listing changes")
		for _, change := range changes.Items {
			changeCopy := change
			newLastChange = changeCopy.Id
			if changeCopy.Id != self.LastChange {
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
	if newLastChange != self.LastChange {
		self.LastChange = newLastChange
		if err = self.saveMetadata(); err != nil {
			return
		}
	}
	return
}

func (self *Futon) mergeChanges() (err error) {
	if atomic.LoadInt64(&self.lastChangeCheck) > time.Now().Add(-time.Second).UnixNano() {
		return
	}
	atomic.StoreInt64(&self.lastChangeCheck, time.Now().UnixNano())
	changes, err := self.changeList()
	if err != nil {
		return
	}
	if err = self.db.Update(func(tx *bolt.Tx) (err error) {
		nodePathById, err := tx.CreateBucketIfNotExists(nodePathById)
		if err != nil {
			self.fatal("While trying to create nodePathById bucket: %v", err)
			return
		}
		nodeByPath, err := tx.CreateBucketIfNotExists(nodeByPath)
		if err != nil {
			self.fatal("While trying to create nodeByPath bucket: %v", err)
			return
		}
		childPathsByPath, err := tx.CreateBucketIfNotExists(childPathsByPath)
		if err != nil {
			self.fatal("While trying to create childPathsByPath bucket: %v", err)
			return
		}
		for _, change := range changes {
			if change.File.Labels.Trashed {
				pathData := nodePathById.Get([]byte(change.File.Id))
				if pathData != nil {
					p := path{}
					(&p).unmarshal(pathData)
					if err = nodePathById.Delete([]byte(change.File.Id)); err != nil {
						self.fatal("While trying to delete %#v: %v", change.File.Id, err)
						return
					}
					if err = nodeByPath.Delete(pathData); err != nil {
						self.fatal("While trying to delete %s: %v", pathData, err)
						return
					}
					if childPaths := childPathsByPath.Bucket(p.parent().marshal()); childPaths != nil {
						if err = childPaths.Delete(p.marshal()); err != nil {
							self.fatal("While trying to delete %s: %v", p.marshal(), err)
							return
						}
					}
					self.debug("%s was removed", p)
				}
			} else {
				for _, parentRef := range change.File.Parents {
					pathData := nodePathById.Get([]byte(parentRef.Id))
					if pathData != nil {
						p := path{}
						(&p).unmarshal(pathData)
						n := self.newNode(p.add(change.File.Title), change.File)
						if err = n.rawSave(tx); err != nil {
							return
						}
						self.debug("%v was updated", n.Path)
					}
				}
			}
		}
		return
	}); err != nil {
		return
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
		self.fatal("While trying to exchange %#v for a refresh token: %v", code, err)
		return
	}
	return
}

var meta = []byte("meta")

func (self *Futon) authorize() (err error) {
	if err = self.db.Update(func(tx *bolt.Tx) (err error) {
		bucket, err := tx.CreateBucketIfNotExists(meta)
		if err != nil {
			self.fatal("While trying to create meta bucket: %v", err)
			return
		}
		metaData := bucket.Get(meta)
		if metaData == nil {
			if err = self.acquireToken(); err != nil {
				return
			}
			if metaData, err = json.Marshal(self); err != nil {
				self.fatal("While trying to JSON encode %+v: %v", self, err)
				return
			}
			if err = bucket.Put(meta, metaData); err != nil {
				self.fatal("While trying to create save %+v: %v", self, err)
				return
			}
		} else {
			if err = json.Unmarshal(metaData, self); err != nil {
				self.fatal("While trying to JSON decode %s: %v", metaData, err)
				return
			}
		}
		return
	}); err != nil {
		return
	}
	transport := &oauth.Transport{
		Config:    config,
		Token:     self.Token,
		Transport: http.DefaultTransport,
	}
	self.client = transport.Client()
	if self.drive, err = drive.New(self.client); err != nil {
		self.fatal("While trying to connect to Drive: %v", err)
		return
	}

	if self.RootFolderId == "" || self.LastChange == 0 {
		var about *drive.About
		self.debug("Loading About")
		if about, err = self.drive.About.Get().Do(); err != nil {
			self.fatal("While trying to get About: %v", err)
			return
		}
		self.debug("Done loading About")
		self.RootFolderId = about.RootFolderId
		self.LastChange = about.LargestChangeId
	}

	self.info("Connected to Google Drive")

	return
}

func (self *Futon) newNode(path path, f *drive.File) (result *node) {
	atime, _ := time.Parse(time.RFC3339, f.LastViewedByMeDate)
	mtime, _ := time.Parse(time.RFC3339, f.ModifiedDate)
	result = &node{
		futon: self,
		Path:  path,
		Id:    f.Id,
		Mode:  os.FileMode(0400),
		Size:  f.FileSize,
		Atime: atime,
		Mtime: mtime,
	}
	if f.MimeType == "application/vnd.google-apps.folder" {
		result.Mode |= 0100 | os.ModeDir
	}
	return
}

func (self *Futon) children(p path) (result []*node, err error) {
	if err = self.db.View(func(tx *bolt.Tx) (err error) {
		childPaths := []path{}
		if bucket := tx.Bucket(childPathsByPath); bucket != nil {
			if children := bucket.Bucket(p.marshal()); children != nil {
				cursor := children.Cursor()
				for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
					childPath := &path{}
					childPath.unmarshal(value)
					childPaths = append(childPaths, *childPath)
				}
			}
		}
		if bucket := tx.Bucket(nodeByPath); bucket != nil {
			for _, childPath := range childPaths {
				if data := bucket.Get(childPath.marshal()); data != nil {
					n := &node{
						futon: self,
					}
					if err = json.Unmarshal(data, n); err != nil {
						self.fatal("While trying to JSON decode %s: %v", data, err)
						return
					}
					result = append(result, n)
				}
			}
		}
		return
	}); err != nil {
		return
	}
	return
}

func (self *Futon) root() (result *node, err error) {
	if err = self.db.View(func(tx *bolt.Tx) (err error) {
		if bucket := tx.Bucket(nodeByPath); bucket != nil {
			var rootPath path
			if data := bucket.Get(rootPath.marshal()); data != nil {
				result = &node{
					futon: self,
				}
				if err = json.Unmarshal(data, result); err != nil {
					self.fatal("While trying to JSON decode %s: %v", data, err)
					return
				}
				return
			}
		}
		return
	}); err != nil || result != nil {
		return
	}
	result = &node{
		futon: self,
		Id:    self.RootFolderId,
		Mode:  0500 | os.ModeDir,
	}
	if err = result.save(); err != nil {
		return
	}
	return
}

func (self *Futon) Root() (result fs.Node, ferr fuse.Error) {
	result, err := self.root()
	if err != nil {
		ferr = fuse.Error(err)
		return
	}
	return
}

func (self *Futon) fatal(format string, params ...interface{}) {
	if self.loglevel > 0 {
		self.log(fmt.Sprintf("FATAL\t%v", format), params...)
	}
}

func (self *Futon) warn(format string, params ...interface{}) {
	if self.loglevel > 1 {
		self.log(fmt.Sprintf("WARN\t%v", format), params...)
	}
}

func (self *Futon) info(format string, params ...interface{}) {
	if self.loglevel > 2 {
		self.log(fmt.Sprintf("INFO\t%v", format), params...)
	}
}

func (self *Futon) debug(format string, params ...interface{}) {
	if self.loglevel > 3 {
		self.log(fmt.Sprintf("DEBUG\t%v", format), params...)
	}
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

func (self *Futon) Loglevel(i int) *Futon {
	self.loglevel = i
	return self
}

func (self *Futon) unmount() {
	if atomic.CompareAndSwapInt32(&self.unmounted, 0, 1) {
		if err := fuse.Unmount(self.mountpoint); err != nil {
			self.fatal("While trying to unmount %#v: %v", self.mountpoint, err)
		}
		if err := self.conn.Close(); err != nil {
			self.fatal("While trying to close %#v: %v", self.mountpoint, err)
		}
	}
	self.info("Unmounted %#v", self.mountpoint)
}

func (self *Futon) Mount() (err error) {
	if err = os.MkdirAll(self.dir, 0700); err != nil {
		return
	}
	if self.db, err = bolt.Open(filepath.Join(self.dir, "futon.db"), 0600, nil); err != nil {
		return
	}

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
		self.fatal("While trying to mount %#v: %v", self.mountpoint, err)
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

	self.info("Mounted %#v", self.mountpoint)
	if err = fs.Serve(self.conn, self); err != nil {
		self.fatal("While trying to serve %#v: %v", self.mountpoint, err)
		return
	}

	<-self.conn.Ready
	if err = self.conn.MountError; err != nil {
		self.fatal("While serving %#v: %v", self.mountpoint, err)
		return
	}

	return
}

func New(mountpoint, dir string, authorizer func(url string) (code string)) (result *Futon) {
	result = &Futon{
		mountpoint: mountpoint,
		dir:        dir,
		authorizer: authorizer,
	}
	return
}

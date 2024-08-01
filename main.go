package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

// const version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := &Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", dir)

	return driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, resources string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection - no place to save records")
	}

	if resources == "" {
		return fmt.Errorf("missing resources - unable to save record (no name)")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resources+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resources string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection - no place to read record")
	}

	if resources == "" {
		return fmt.Errorf("missing resources - no place to read record")
	}

	record := filepath.Join(d.dir, collection, resources)

	if _, err := stat(record); err != nil {
		return err
	}

	b, err := os.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("missing collection - no place to read records")
	}

	dir := filepath.Join(d.dir, collection)
	if _, err := stat(dir); err != nil {
		return nil, err
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var records []string

	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Delete(collection, resources string) error {
	path := filepath.Join(collection, resources)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file or directory named %v", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}

	return
}

type User struct {
	Name    string      `json:"name"`
	Age     json.Number `json:"age"`
	Contact string      `json:"contact"`
	Company string      `json:"company"`
	Address Address     `json:"address"`
}

type Address struct {
	City    string      `json:"city"`
	State   string      `json:"state"`
	Country string      `json:"country"`
	Pincode json.Number `json:"pincode"`
}

func main() {

	dir := "./"
	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error:", err)
	}

	employees := []User{
		{
			"John", "23", "2324243424", "Murl Tech", Address{"Banglore", "Karnataka", "India", "274372"},
		},
		{
			"Paul", "23", "2324243424", "Murl Tech", Address{"Banglore", "Karnataka", "India", "274372"},
		},
		{
			"Mohan", "23", "2324243424", "Murl Tech", Address{"Banglore", "Karnataka", "India", "274372"},
		},
		{
			"James", "23", "2324243424", "Murl Tech", Address{"Banglore", "Karnataka", "India", "274372"},
		},
		{
			"Jerry", "23", "2324243424", "Murl Tech", Address{"Banglore", "Karnataka", "India", "274372"},
		},
	}

	for _, value := range employees {
		db.Write("users", value.Name, User{
			Name:    value.Name,
			Contact: value.Contact,
			Age:     value.Age,
			Company: value.Company,
			Address: value.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error:", err)
	}

	fmt.Println("Records:", records)

	allUsers := []User{}

	for _, f := range records {
		employeesFound := User{}

		if err := json.Unmarshal([]byte(f), &employeesFound); err != nil {
			fmt.Println("Error:", err)
		}

		allUsers = append(allUsers, employeesFound)

	}

	fmt.Println(allUsers)

	// Delete One User
	// if err := db.Delete("users", "John"); err != nil {
	// 	fmt.Println("Error:", err)
	// }

	// Delete All Users
	// if err := db.Delete("users", ""); err != nil {
	// 	fmt.Println("Error:", err)
	// }

}

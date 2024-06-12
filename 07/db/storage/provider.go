package storage

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

type Provider struct {
	dataDir string
	fileNum int
}

type FileType int

const (
	FileTypeUnknown FileType = iota
	FileTypeSSTable
	FileTypeWAL
)

type FileMetadata struct {
	fileNum  int
	fileType FileType
}

func (f *FileMetadata) IsSSTable() bool {
	return f.fileType == FileTypeSSTable
}

func (f *FileMetadata) IsWAL() bool {
	return f.fileType == FileTypeWAL
}

func (f *FileMetadata) FileNum() int {
	return f.fileNum
}

func NewProvider(dataDir string) (*Provider, error) {
	s := &Provider{dataDir: dataDir}

	err := s.ensureDataDirExists()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Provider) ensureDataDirExists() error {
	err := os.MkdirAll(s.dataDir, 0755)
	if err != nil {
		return err
	}
	return nil
}

func (s *Provider) ListFiles() ([]*FileMetadata, error) {
	files, err := os.ReadDir(s.dataDir)
	if err != nil {
		return nil, err
	}
	var meta []*FileMetadata
	var fileNumber int
	var fileExtension string
	for _, f := range files {
		_, err = fmt.Sscanf(f.Name(), "%06d.%s", &fileNumber, &fileExtension)
		if err != nil {
			return nil, err
		}
		fileType := FileTypeUnknown
		switch fileExtension {
		case "sst":
			fileType = FileTypeSSTable
		case "log":
			fileType = FileTypeWAL
		}
		meta = append(meta, &FileMetadata{
			fileNum:  fileNumber,
			fileType: fileType,
		})
		if fileNumber >= s.fileNum {
			s.fileNum = fileNumber
		}
	}
	slices.SortFunc(meta, func(a, b *FileMetadata) int {
		return cmp.Compare(a.fileNum, b.fileNum)
	})
	return meta, nil
}

func (s *Provider) nextFileNum() int {
	s.fileNum++
	return s.fileNum
}

func (s *Provider) makeFileName(fileNumber int, fileType FileType) string {
	switch fileType {
	case FileTypeSSTable:
		return fmt.Sprintf("%06d.sst", fileNumber)
	case FileTypeWAL:
		return fmt.Sprintf("%06d.log", fileNumber)
	case FileTypeUnknown:
	}
	panic("unknown file type")
}

func (s *Provider) prepareNewFile(fileType FileType) *FileMetadata {
	return &FileMetadata{
		fileNum:  s.nextFileNum(),
		fileType: fileType,
	}
}

func (s *Provider) PrepareNewSSTFile() *FileMetadata {
	return s.prepareNewFile(FileTypeSSTable)
}

func (s *Provider) PrepareNewWALFile() *FileMetadata {
	return s.prepareNewFile(FileTypeWAL)
}

func (s *Provider) OpenFileForWriting(meta *FileMetadata) (*os.File, error) {
	const openFlags = os.O_RDWR | os.O_CREATE | os.O_EXCL
	filename := s.makeFileName(meta.fileNum, meta.fileType)
	file, err := os.OpenFile(filepath.Join(s.dataDir, filename), openFlags, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (s *Provider) OpenFileForReading(meta *FileMetadata) (*os.File, error) {
	const openFlags = os.O_RDONLY
	filename := s.makeFileName(meta.fileNum, meta.fileType)
	file, err := os.OpenFile(filepath.Join(s.dataDir, filename), openFlags, 0)
	if err != nil {
		return nil, err
	}
	return file, nil
}
func (s *Provider) DeleteFile(meta *FileMetadata) error {
	name := s.makeFileName(meta.fileNum, meta.fileType)
	path := filepath.Join(s.dataDir, name)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

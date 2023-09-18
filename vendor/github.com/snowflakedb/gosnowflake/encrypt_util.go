// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

type snowflakeFileEncryption struct {
	QueryStageMasterKey string `json:"queryStageMasterKey,omitempty"`
	QueryID             string `json:"queryId,omitempty"`
	SMKID               int64  `json:"smkId,omitempty"`
}

// PUT requests return a single encryptionMaterial object whereas GET requests
// return a slice (array) of encryptionMaterial objects, both under the field
// 'encryptionMaterial'
type encryptionWrapper struct {
	snowflakeFileEncryption
	EncryptionMaterials []snowflakeFileEncryption
}

// override default behavior for wrapper
func (ew *encryptionWrapper) UnmarshalJSON(data []byte) error {
	// if GET, unmarshal slice of encryptionMaterial
	if err := json.Unmarshal(data, &ew.EncryptionMaterials); err == nil {
		return err
	}
	// else (if PUT), unmarshal the encryptionMaterial itself
	return json.Unmarshal(data, &ew.snowflakeFileEncryption)
}

type encryptMetadata struct {
	key     string
	iv      string
	matdesc string
}

// encryptStream encrypts a stream buffer using AES128 block cipher in CBC mode
// with PKCS5 padding
func encryptStream(
	sfe *snowflakeFileEncryption,
	src io.Reader,
	out io.Writer,
	chunkSize int) (*encryptMetadata, error) {
	if chunkSize == 0 {
		chunkSize = aes.BlockSize * 4 * 1024
	}
	decodedKey, err := base64.StdEncoding.DecodeString(sfe.QueryStageMasterKey)
	if err != nil {
		return nil, err
	}
	keySize := len(decodedKey)

	fileKey := getSecureRandom(keySize)
	block, err := aes.NewCipher(fileKey)
	if err != nil {
		return nil, err
	}
	ivData := getSecureRandom(block.BlockSize())

	mode := cipher.NewCBCEncrypter(block, ivData)
	cipherText := make([]byte, chunkSize)
	chunk := make([]byte, chunkSize)

	// encrypt file with CBC
	padded := false
	for {
		// read the stream buffer up to len(chunk) bytes into chunk
		// note that all spaces in chunk may be used even if Read() returns n < len(chunk)
		n, err := src.Read(chunk)
		if n == 0 || err != nil {
			break
		} else if n%aes.BlockSize != 0 {
			// add padding to the end of the chunk and update the length n
			chunk = padBytesLength(chunk[:n], aes.BlockSize)
			n = len(chunk)
			padded = true
		}
		// make sure only n bytes of chunk is used
		mode.CryptBlocks(cipherText, chunk[:n])
		out.Write(cipherText[:n])
	}
	if err != nil {
		return nil, err
	}

	// add padding if not yet added
	if !padded {
		padding := bytes.Repeat([]byte(string(rune(aes.BlockSize))), aes.BlockSize)
		mode.CryptBlocks(cipherText, padding)
		out.Write(cipherText[:len(padding)])
	}

	// encrypt key with ECB
	fileKey = padBytesLength(fileKey, block.BlockSize())
	encryptedFileKey := make([]byte, len(fileKey))
	if err = encryptECB(encryptedFileKey, fileKey, decodedKey); err != nil {
		return nil, err
	}

	matDesc := materialDescriptor{
		strconv.Itoa(int(sfe.SMKID)),
		sfe.QueryID,
		strconv.Itoa(keySize * 8),
	}

	matDescUnicode, err := matdescToUnicode(matDesc)
	if err != nil {
		return nil, err
	}
	return &encryptMetadata{
		base64.StdEncoding.EncodeToString(encryptedFileKey),
		base64.StdEncoding.EncodeToString(ivData),
		matDescUnicode,
	}, nil
}

func encryptECB(encrypted []byte, fileKey []byte, decodedKey []byte) error {
	block, err := aes.NewCipher(decodedKey)
	if err != nil {
		return err
	}
	if len(fileKey)%block.BlockSize() != 0 {
		return fmt.Errorf("input not full of blocks")
	}
	if len(encrypted) < len(fileKey) {
		return fmt.Errorf("output length is smaller than input length")
	}
	for len(fileKey) > 0 {
		block.Encrypt(encrypted, fileKey[:block.BlockSize()])
		encrypted = encrypted[block.BlockSize():]
		fileKey = fileKey[block.BlockSize():]
	}
	return nil
}

func decryptECB(decrypted []byte, keyBytes []byte, decodedKey []byte) error {
	block, err := aes.NewCipher(decodedKey)
	if err != nil {
		return err
	}
	if len(keyBytes)%block.BlockSize() != 0 {
		return fmt.Errorf("input not full of blocks")
	}
	if len(decrypted) < len(keyBytes) {
		return fmt.Errorf("output length is smaller than input length")
	}
	for len(keyBytes) > 0 {
		block.Decrypt(decrypted, keyBytes[:block.BlockSize()])
		keyBytes = keyBytes[block.BlockSize():]
		decrypted = decrypted[block.BlockSize():]
	}
	return nil
}

func encryptFile(
	sfe *snowflakeFileEncryption,
	filename string,
	chunkSize int,
	tmpDir string) (
	*encryptMetadata, string, error) {
	if chunkSize == 0 {
		chunkSize = aes.BlockSize * 4 * 1024
	}
	tmpOutputFile, err := os.CreateTemp(tmpDir, baseName(filename)+"#")
	if err != nil {
		return nil, "", err
	}
	infile, err := os.OpenFile(filename, os.O_CREATE|os.O_RDONLY, readWriteFileMode)
	if err != nil {
		return nil, "", err
	}
	meta, err := encryptStream(sfe, infile, tmpOutputFile, chunkSize)
	if err != nil {
		return nil, "", err
	}
	return meta, tmpOutputFile.Name(), nil
}

func decryptFile(
	metadata *encryptMetadata,
	sfe *snowflakeFileEncryption,
	filename string,
	chunkSize int,
	tmpDir string) (
	string, error) {
	if chunkSize == 0 {
		chunkSize = aes.BlockSize * 4 * 1024
	}
	decodedKey, err := base64.StdEncoding.DecodeString(sfe.QueryStageMasterKey)
	if err != nil {
		return "", err
	}
	keyBytes, err := base64.StdEncoding.DecodeString(metadata.key) // encrypted file key
	if err != nil {
		return "", err
	}
	ivBytes, err := base64.StdEncoding.DecodeString(metadata.iv)
	if err != nil {
		return "", err
	}

	// decrypt file key
	decryptedKey := make([]byte, len(keyBytes))
	if err = decryptECB(decryptedKey, keyBytes, decodedKey); err != nil {
		return "", err
	}
	decryptedKey, err = paddingTrim(decryptedKey)
	if err != nil {
		return "", err
	}

	// decrypt file
	block, err := aes.NewCipher(decryptedKey)
	if err != nil {
		return "", err
	}
	mode := cipher.NewCBCDecrypter(block, ivBytes)

	tmpOutputFile, err := os.CreateTemp(tmpDir, baseName(filename)+"#")
	if err != nil {
		return "", err
	}
	defer tmpOutputFile.Close()
	infile, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer infile.Close()
	var totalFileSize int
	var prevChunk []byte
	for {
		chunk := make([]byte, chunkSize)
		n, err := infile.Read(chunk)
		if n == 0 || err != nil {
			break
		} else if n%aes.BlockSize != 0 {
			// add padding to the end of the chunk and update the length n
			chunk = padBytesLength(chunk[:n], aes.BlockSize)
			n = len(chunk)
		}
		totalFileSize += n
		chunk = chunk[:n]
		mode.CryptBlocks(chunk, chunk)
		tmpOutputFile.Write(chunk)
		prevChunk = chunk
	}
	if err != nil {
		return "", err
	}
	if prevChunk != nil {
		totalFileSize -= paddingOffset(prevChunk)
	}
	tmpOutputFile.Truncate(int64(totalFileSize))
	return tmpOutputFile.Name(), nil
}

type materialDescriptor struct {
	SmkID   string `json:"smkId"`
	QueryID string `json:"queryId"`
	KeySize string `json:"keySize"`
}

func matdescToUnicode(matdesc materialDescriptor) (string, error) {
	s, err := json.Marshal(&matdesc)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

func getSecureRandom(byteLength int) []byte {
	token := make([]byte, byteLength)
	rand.Read(token)
	return token
}

func padBytesLength(src []byte, blockSize int) []byte {
	padLength := blockSize - len(src)%blockSize
	padText := bytes.Repeat([]byte{byte(padLength)}, padLength)
	return append(src, padText...)
}

func paddingTrim(src []byte) ([]byte, error) {
	unpadding := src[len(src)-1]
	n := int(unpadding)
	if n == 0 || n > len(src) {
		return nil, &SnowflakeError{
			Number:  ErrInvalidPadding,
			Message: errMsgInvalidPadding,
		}
	}
	return src[:len(src)-n], nil
}

func paddingOffset(src []byte) int {
	length := len(src)
	return int(src[length-1])
}

type contentKey struct {
	KeyID         string `json:"KeyId,omitempty"`
	EncryptionKey string `json:"EncryptedKey,omitempty"`
	Algorithm     string `json:"Algorithm,omitempty"`
}

type encryptionAgent struct {
	Protocol            string `json:"Protocol,omitempty"`
	EncryptionAlgorithm string `json:"EncryptionAlgorithm,omitempty"`
}

type keyMetadata struct {
	EncryptionLibrary string `json:"EncryptionLibrary,omitempty"`
}

type encryptionData struct {
	EncryptionMode      string          `json:"EncryptionMode,omitempty"`
	WrappedContentKey   contentKey      `json:"WrappedContentKey,omitempty"`
	EncryptionAgent     encryptionAgent `json:"EncryptionAgent,omitempty"`
	ContentEncryptionIV string          `json:"ContentEncryptionIV,omitempty"`
	KeyWrappingMetadata keyMetadata     `json:"KeyWrappingMetadata,omitempty"`
}

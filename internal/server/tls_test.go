package server

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	// WHEN
	certPEM, keyPEM, err := generateSelfSignedCert()

	// THEN
	require.NoError(t, err)
	assert.NotEmpty(t, certPEM)
	assert.NotEmpty(t, keyPEM)

	// 生成した PEM をパースできる
	_, err = tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
}

func TestLoadOrGenerateTLSConfig_GenerateNew(t *testing.T) {
	// GIVEN
	dataDir := t.TempDir()

	// WHEN
	config, err := loadOrGenerateTLSConfig(dataDir)

	// THEN
	require.NoError(t, err)
	assert.NotNil(t, config)
	assert.Len(t, config.Certificates, 1)

	// ファイルが作成されている
	assert.FileExists(t, filepath.Join(dataDir, certFileName))
	assert.FileExists(t, filepath.Join(dataDir, keyFileName))
}

func TestLoadOrGenerateTLSConfig_LoadExisting(t *testing.T) {
	// GIVEN: 事前に証明書を生成しておく
	dataDir := t.TempDir()
	config1, err := loadOrGenerateTLSConfig(dataDir)
	require.NoError(t, err)

	// WHEN: 同じディレクトリで再度呼び出す
	config2, err := loadOrGenerateTLSConfig(dataDir)

	// THEN: エラーなく読み込める
	require.NoError(t, err)
	assert.NotNil(t, config2)

	// 同じ証明書が読み込まれる (DER バイト列が一致)
	assert.Equal(t,
		config1.Certificates[0].Certificate[0],
		config2.Certificates[0].Certificate[0],
	)
}

func TestLoadOrGenerateTLSConfig_FilePermissions(t *testing.T) {
	// GIVEN
	dataDir := t.TempDir()
	_, err := loadOrGenerateTLSConfig(dataDir)
	require.NoError(t, err)

	// THEN: 秘密鍵ファイルのパーミッションが 0600
	keyInfo, err := os.Stat(filepath.Join(dataDir, keyFileName))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), keyInfo.Mode().Perm())

	// THEN: 証明書ファイルのパーミッションが 0600
	certInfo, err := os.Stat(filepath.Join(dataDir, certFileName))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), certInfo.Mode().Perm())
}

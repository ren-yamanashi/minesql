package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

const (
	certFileName = "server-cert.pem"
	keyFileName  = "server-key.pem"
)

// loadOrGenerateTLSConfig はデータディレクトリから証明書を読み込む
//
// 証明書が存在しない場合は自己署名証明書を生成して保存する
func loadOrGenerateTLSConfig(dataDir string) (*tls.Config, error) {
	certPath := filepath.Join(dataDir, certFileName)
	keyPath := filepath.Join(dataDir, keyFileName)

	// 既存の証明書があればそれを読み込む
	if fileExists(certPath) && fileExists(keyPath) {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate: %w", err)
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}, nil
	}

	// 自己署名証明書を生成
	certPEM, keyPEM, err := generateSelfSignedCert()
	if err != nil {
		return nil, fmt.Errorf("failed to generate certificate: %w", err)
	}

	// ファイルに保存
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		return nil, fmt.Errorf("failed to write certificate: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return nil, fmt.Errorf("failed to write key: %w", err)
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated certificate: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// generateSelfSignedCert は ECDSA P-256 鍵ペアと自己署名証明書を生成する
func generateSelfSignedCert() (certPEM []byte, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "MineSQL Server"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour), // 10 年
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

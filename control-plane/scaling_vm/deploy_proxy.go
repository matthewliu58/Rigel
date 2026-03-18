package scaling_vm

import (
	"control-plane/util"
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"
)

var (
	username        string
	localPathProxy  string
	remotePathProxy string
	binaryProxy     string
	localPathPlane  string
	remotePathPlane string
	binaryPlane     string
	privateKey      string
)

func InitScalingConfig() {
	uu := util.Config_ // 这里假设你已经加载了 Config

	username = uu.Scaling.Username
	localPathProxy = uu.Scaling.LocalPathProxy
	remotePathProxy = uu.Scaling.RemotePathProxy
	binaryProxy = uu.Scaling.BinaryProxy
	localPathPlane = uu.Scaling.LocalPathPlane
	remotePathPlane = uu.Scaling.RemotePathPlane
	binaryPlane = uu.Scaling.BinaryPlane
	privateKey = uu.Scaling.PrivateKey
}

// SSHConfig 包含 SSH 连接所需的配置信息
type SSHConfig struct {
	Username   string
	Host       string
	Port       string
	PrivateKey string // 私钥文件路径
}

// sshToMatthAndDeployBinary SSH 连接到服务器，上传二进制文件并启动它
//func sshToDeployBinary(config *SSHConfig, localPath, remotePath, binaryString, pre string, logger *slog.Logger) error {
//	// 创建 SSH 客户端配置，使用系统默认的 SSH 配置
//	clientConfig := &ssh.ClientConfig{
//		User:            config.Username,
//		Auth:            []ssh.AuthMethod{ssh.PublicKeysCallback(agentCallback())}, // 使用默认 SSH 密钥
//		HostKeyCallback: ssh.InsecureIgnoreHostKey(),                               // 忽略主机密钥验证（生产环境中应谨慎）
//	}
//
//	logger.Info("sshToDeployBinary", slog.String("pre", pre))
//
//	// 连接到 SSH 服务器
//	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%s", config.Host, config.Port), clientConfig)
//	if err != nil {
//		return fmt.Errorf("failed to dial: %v", err)
//	}
//	defer conn.Close()
//
//	logger.Info("ssh Dial success", slog.String("pre", pre))
//
//	// 打开远程会话
//	session, err := conn.NewSession()
//	if err != nil {
//		return fmt.Errorf("failed to create session: %v", err)
//	}
//	defer session.Close()
//
//	logger.Info("NewSession success", slog.String("pre", pre))
//
//	// 读取本地二进制文件
//	//data, err := ioutil.ReadFile(localBinaryPath)
//	//if err != nil {
//	//	return fmt.Errorf("failed to read binary file: %v", err)
//	//}
//
//	// 上传二进制文件到远程服务器的 /home/matth 目录
//	err = UploadDirSFTP(conn, localPath, remotePath)
//	if err != nil {
//		return fmt.Errorf("failed to upload binary: %v", err)
//	}
//
//	logger.Info("UploadDirSFTP success", slog.String("pre", pre))
//
//	// 执行远程命令来启动二进制文件
//	err = startBinaryInBackground(session, remotePath, binaryString, logger)
//	if err != nil {
//		return fmt.Errorf("failed to start binary: %v", err)
//	}
//
//	logger.Info("startBinaryInBackground success", slog.String("pre", pre))
//
//	return nil
//}

func sshToDeployBinary(config *SSHConfig, localPath_, remotePath_, binaryString_,
	pre string, logger *slog.Logger) error {

	logger.Info("sshToDeployBinary", slog.String("pre", pre))

	// === 1. 读取本地私钥文件 ===
	key, err := os.ReadFile(config.PrivateKey)
	if err != nil {
		return fmt.Errorf("read private key failed: %v", err)
	}

	logger.Info("read private key success", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return fmt.Errorf("parse private key failed: %v", err)
	}

	logger.Info("parse private key success", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	clientConfig := &ssh.ClientConfig{
		User: config.Username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 生产环境请改为验证 host key
	}

	// === 2. TCP Dial 加 5s 超时 ===
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	tcpConn, err := dialer.Dial("tcp", net.JoinHostPort(config.Host, config.Port))
	if err != nil {
		return fmt.Errorf("failed to dial TCP: %v", err)
	}

	logger.Info("tcp Dial success", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	// === 3. 建立 SSH 连接 ===
	conn, chans, reqs, err := ssh.NewClientConn(tcpConn, net.JoinHostPort(config.Host, config.Port), clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create SSH client: %v", err)
	}
	defer conn.Close()

	logger.Info("ssh Dial success", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	client := ssh.NewClient(conn, chans, reqs)
	defer client.Close()

	logger.Info("ssh new NewClient", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	// === 4. 上传文件 ===
	err = UploadDirSFTP(client, localPath_, remotePath_)
	if err != nil {
		return fmt.Errorf("failed to upload binary: %v", err)
	}
	logger.Info("UploadDirSFTP success", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	// === 5. 启动远程二进制文件 ===
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create session: %v", err)
	}
	defer session.Close()

	logger.Info("NewSession success", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	err = startBinaryInBackground(session, remotePath_, binaryString_, pre, logger)
	if err != nil {
		return fmt.Errorf("failed to start binary: %v", err)
	}
	logger.Info("startBinaryInBackground success", slog.String("pre", pre),
		slog.String("binaryString", binaryString_))

	return nil
}

//// agentCallback 用于获取默认 SSH agent 中的密钥
//func agentCallback() func() ([]ssh.Signer, error) {
//	// 返回一个闭包函数，符合 PublicKeysCallback 的要求
//	return func() ([]ssh.Signer, error) {
//		// 获取 SSH agent
//		sshAgent := agent.NewClient(os.Stdin) // 使用 os.Stdin 连接到默认的 SSH agent
//		if sshAgent == nil {
//			return nil, fmt.Errorf("failed to connect to SSH agent")
//		}
//
//		// 获取密钥列表
//		keys, err := sshAgent.List()
//		if err != nil {
//			return nil, fmt.Errorf("failed to list keys: %v", err)
//		}
//
//		if len(keys) == 0 {
//			return nil, fmt.Errorf("no keys found in the agent")
//		}
//
//		// 将 ssh.Key 转换为 ssh.Signer
//		var signers []ssh.Signer
//		for _, key := range keys {
//			signer, err := ssh.NewSignerFromKey(key)
//			if err != nil {
//				return nil, fmt.Errorf("failed to create signer: %v", err)
//			}
//			signers = append(signers, signer)
//		}
//
//		return signers, nil
//	}
//}

// uploadBinaryToRemote 上传二进制文件到远程服务器
func UploadDirSFTP(sshClient *ssh.Client, localDir, remoteDir string) error {
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return fmt.Errorf("create sftp client failed: %w", err)
	}
	defer sftpClient.Close()

	// 确保远端根目录存在
	if err := sftpClient.MkdirAll(remoteDir); err != nil {
		return fmt.Errorf("create remote dir failed: %w", err)
	}

	return filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(localDir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		remotePath := filepath.Join(remoteDir, relPath)

		if info.IsDir() {
			// 创建远端目录
			return sftpClient.MkdirAll(remotePath)
		}

		// 打开本地文件
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		// 创建远端文件
		dstFile, err := sftpClient.Create(remotePath)
		if err != nil {
			return err
		}
		defer dstFile.Close()

		// 复制内容
		if _, err := io.Copy(dstFile, srcFile); err != nil {
			return err
		}

		// 保留权限（可选但推荐）
		if err := sftpClient.Chmod(remotePath, info.Mode()); err != nil {
			return err
		}

		return nil
	})
}

// startBinaryInBackground 在远程服务器上启动二进制文件，且不阻塞
func startBinaryInBackground(
	session *ssh.Session,
	remotePath_ string,
	binaryString_ string,
	pre string,
	logger *slog.Logger,
) error {

	// 基本防御
	if remotePath_ == "" || binaryString_ == "" {
		return fmt.Errorf("remotePath or binaryString is empty")
	}

	cmd := fmt.Sprintf(
		`cd %q && test -x %q && nohup ./%q > nohup.out 2>&1 < /dev/null & >/dev/null 2>&1`,
		remotePath_,
		binaryString_,
		binaryString_,
	)

	logger.Info("Starting remote binary",
		slog.String("pre", pre),
		slog.String("workdir", remotePath_),
		slog.String("binary", binaryString_),
		slog.String("cmd", cmd),
	)

	// 用 goroutine 包一层，防止 SSH 永久阻塞
	errCh := make(chan error, 1)

	go func() {
		// Run 可能永远不返回
		errCh <- session.Run(cmd)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("failed to start binary in background: %w", err)
		}
		logger.Info("Binary started (ssh session returned)",
			slog.String("pre", pre),
			slog.String("binary", binaryString_),
		)

	case <-time.After(3 * time.Second):
		//超时放行 —— 这是预期行为
		logger.Warn("SSH session did not return, assume binary started",
			slog.String("pre", pre),
			slog.String("binary", binaryString_),
		)
	}

	return nil
}

// deployBinaryToServer 这个函数将配置与文件路径作为输入，执行 SSH 连接、文件上传和启动操作
func deployBinaryToServer(username, host, port, localPath, remotePath, binaryString, pre string, logger *slog.Logger) error {
	// 创建 SSH 配置
	config := &SSHConfig{
		Username:   username,
		Host:       host,
		Port:       port,
		PrivateKey: privateKey,
	}
	// 调用 SSH 连接并部署二进制文件
	return sshToDeployBinary(config, localPath, remotePath, binaryString, pre, logger)
}

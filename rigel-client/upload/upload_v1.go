package upload

// =========================
// HTTPS Direct Upload using Service Account JSON
// =========================
//func UploadToGCSbyDirectHttps(localFilePath, bucketName, objectName, credFile string, logger *slog.Logger) error {
//
//	logger.Info("Uploading file to GCS bucket using direct HTTPS upload", localFilePath, objectName)
//
//	ctx := context.Background()
//
//	// 从 Service Account JSON 获取 Token
//	jsonBytes, err := os.ReadFile(credFile)
//	if err != nil {
//		return fmt.Errorf("failed to read credentials file: %w", err)
//	}
//
//	creds, err := google.CredentialsFromJSON(ctx, jsonBytes, "https://www.googleapis.com/auth/devstorage.full_control")
//	if err != nil {
//		return fmt.Errorf("failed to parse credentials: %w", err)
//	}
//
//	token, err := creds.TokenSource.Token()
//	if err != nil {
//		return fmt.Errorf("failed to get access token: %w", err)
//	}
//	accessToken := token.AccessToken
//
//	// 打开本地文件
//	f, err := os.Open(localFilePath)
//	if err != nil {
//		return fmt.Errorf("failed to open local file: %w", err)
//	}
//	defer f.Close()
//
//	// 构造 PUT 请求
//	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucketName, objectName)
//	req, err := http.NewRequest("PUT", url, f)
//	if err != nil {
//		return fmt.Errorf("failed to create HTTP request: %w", err)
//	}
//
//	req.Header.Set("Authorization", "Bearer "+accessToken)
//	req.Header.Set("Content-Type", "application/octet-stream")
//
//	client := &http.Client{Timeout: 5 * time.Minute}
//	resp, err := client.Do(req)
//	if err != nil {
//		return fmt.Errorf("failed to perform HTTP request: %w", err)
//	}
//	defer resp.Body.Close()
//
//	if resp.StatusCode >= 300 {
//		body, _ := io.ReadAll(resp.Body)
//		return fmt.Errorf("upload failed, status: %d, body: %s", resp.StatusCode, string(body))
//	}
//	logger.Info("Upload success direct HTTPS upload", localFilePath, objectName)
//
//	return nil
//}

//func UploadToGCSbyReDirectHttpsV1(localFilePath, bucketName, fileName, credFile, hops string,
//	reqHeaders http.Header, logger *slog.Logger) error {
//	// 读取 bucket 和 object
//	//bucketName := reqHeaders.Get("X-Bucket-Name")
//	objectName := fileName
//
//	// 生成 access token
//	ctx := context.Background()
//	jsonBytes, err := os.ReadFile(credFile)
//	if err != nil {
//		return fmt.Errorf("failed to read credentials file: %w", err)
//	}
//	creds, err := google.CredentialsFromJSON(ctx, jsonBytes, "https://www.googleapis.com/auth/devstorage.full_control")
//	if err != nil {
//		return fmt.Errorf("failed to parse credentials: %w", err)
//	}
//	token, err := creds.TokenSource.Token()
//	if err != nil {
//		return fmt.Errorf("failed to get access token: %w", err)
//	}
//	accessToken := token.AccessToken
//
//	// 打开本地文件
//	f, err := os.Open(localFilePath)
//	if err != nil {
//		return fmt.Errorf("failed to open local file: %w", err)
//	}
//	defer f.Close()
//
//	//hops := reqHeaders.Get("X-Hops") // "34.69.185.247:8090,136.116.114.219:8080"
//	hopList := strings.Split(hops, ",")
//	if len(hopList) <= 1 {
//		return fmt.Errorf("invalid X-Hops header: %s", hops)
//	}
//	firstHop := hopList[0] // 第一跳 IP:PORT
//
//	// 拼装最终 URI
//	url := fmt.Sprintf("http://%s/%s/%s", firstHop, bucketName, objectName)
//
//	// 构造 PUT 请求
//	putReq, err := http.NewRequest("POST", url, f)
//	if err != nil {
//		return fmt.Errorf("failed to create POST request: %w", err)
//	}
//	putReq.Header.Set("Authorization", "Bearer "+accessToken)
//	putReq.Header.Set("Content-Type", "application/octet-stream")
//	putReq.Header.Set("X-Hops", hops)
//	putReq.Header.Set("X-Index", "1")
//	putReq.Header.Set("X-Rate-Limit-Enable", "false")
//
//	client := &http.Client{Timeout: 5 * time.Minute}
//	resp, err := client.Do(putReq)
//	if err != nil {
//		return fmt.Errorf("failed to upload to GCS: %w", err)
//	}
//	defer resp.Body.Close()
//
//	if resp.StatusCode >= 300 {
//		body, _ := io.ReadAll(resp.Body)
//		return fmt.Errorf("upload failed, status: %d, body: %s", resp.StatusCode, string(body))
//	}
//
//	logger.Info("UploadToGCSbyReDirectHttps success:", bucketName, objectName)
//	return nil
//}

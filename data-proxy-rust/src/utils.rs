use rand::Rng;
use std::process;

/// 生成指定长度的随机字母（对应 Go 的 GenerateRandomLetters）
pub fn generate_random_letters(length: usize) -> String {
    let mut rng = rand::thread_rng();
    const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..LETTERS.len());
            LETTERS[idx] as char
        })
        .collect()
}

/// 拆分 x-hops 字符串（对应 Go 的 splitHops）
pub fn split_hops(hops_str: &str) -> Vec<String> {
    hops_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// 获取当前进程 ID
pub fn get_pid() -> u32 {
    process::id()
}
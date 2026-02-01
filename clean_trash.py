import os
import glob

def clean_xray_files():
    # 1. 设定要匹配的文件模式
    # Xray-linux-64.zip* 会匹配到 .zip, .zip.1, .zip.10 等所有开头相同的文件
    pattern = "Xray-linux-64.zip*"
    
    # 2. 查找所有匹配的文件
    files_to_delete = glob.glob(pattern)
    
    if not files_to_delete:
        print("🎉 太棒了，没有发现垃圾文件，目录很干净！")
        return

    print(f"🧐 发现了 {len(files_to_delete)} 个垃圾文件，准备执行清理...\n")

    # 3. 循环删除
    count = 0
    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            print(f"🗑️ 已删除: {file_path}")
            count += 1
        except Exception as e:
            print(f"❌ 删除失败 {file_path}: {e}")

    print(f"\n✅ 清理完成！共删除了 {count} 个文件。")
    print("⚠️ 注意：这只是删除了本地文件，请务必执行 Git 命令同步到 GitHub！")

if __name__ == "__main__":
    clean_xray_files()

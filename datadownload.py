from kaggle.api.kaggle_api_extended import KaggleApi

def download_with_official_api(dataset_name, save_path):
    api = KaggleApi()
    api.authenticate()
    # 下面这个方法下载整个数据集并解压到 save_path
    api.dataset_metadata(
        dataset=dataset_name,
        path=save_path
    )

if __name__ == "__main__":
    save_path = r"D:\华东师大\实践考核\毕业论文相关\实验分析\Dataset\188-million-us-wildfires"
    download_with_official_api("rtatman/188-million-us-wildfires", save_path)

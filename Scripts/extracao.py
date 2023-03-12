import kaggle


def main():
    #  Faz o download dos dados do Kaggle. Vale lembrar que é necessário ter uma conta no Kaggle e ter feito o download do arquivo kaggle.json para a pasta ~/.kaggle
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        "olistbr/brazilian-ecommerce", path="./Dados", unzip=True
    )


if __name__ == "__main__":
    main()

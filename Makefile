
# Downloads the M5 Kaggle dataset locally (you need to have a valid API key see here: https://github.com/Kaggle/kaggle-api#api-credentials)
download_m5:
	poetry run kaggle competitions download -c m5-forecasting-accuracy
	unzip m5-forecasting-accuracy.zip -d data/m5-forecasting-accuracy
	rm m5-forecasting-accuracy.zip

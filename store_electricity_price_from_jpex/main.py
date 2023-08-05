from datetime import datetime, timedelta
import pytz
import requests
import io
import csv
import polars as pl
from flask import Flask, jsonify, abort
import os
from google.cloud import firestore
import json
import msgpack
from google.cloud import storage

def store_electricity_price_from_jpex(request):
  try:
    # 翌日のデータを取得するため
    tz = pytz.timezone('Asia/Tokyo')
    tomorrow = datetime.now(tz) + timedelta(days=1)

    # データは年後ごとにまとめられるため
    fiscal_year = tomorrow.year
    if month := tomorrow.month < 4:
        fiscal_year -= 1

    # referer偽装してcsvファイル取得
    url = "https://www.jepx.jp/js/csv_read.php?dir=spot_summary&file=spot_summary_" + str(fiscal_year) + ".csv"
    referer = "https://www.jepx.jp/electricpower/market-data/spot/"

    headers = {
        "Referer": referer,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    response = requests.get(url, headers=headers)

    # システムプライス・各エリアプライス取得、登録用マップ
    area_map = {
      'システム・プライス' : {
        'column_name' : 'システムプライス(円/kWh)',
        'db_collection' : 'system'
      },
      '北海道' : {
        'column_name' : 'エリアプライス北海道(円/kWh)',
        'db_collection' : 'hokkaido'
      },
      '東北' : {
        'column_name' : 'エリアプライス東北(円/kWh)',
        'db_collection' : 'tohoku'
      },
      '東京' : {
        'column_name' : 'エリアプライス東京(円/kWh)',
        'db_collection' : 'tokyo'
      },
      '北陸' : {
        'column_name' : 'エリアプライス北陸(円/kWh)',
        'db_collection' : 'hokuriku'
      },
      '中部' : {
        'column_name' : 'エリアプライス中部(円/kWh)',
        'db_collection' : 'chubu'
      },
      '関西' : {
        'column_name' : 'エリアプライス関西(円/kWh)',
        'db_collection' : 'kansai'
      },
      '中国' : {
        'column_name' : 'エリアプライス中国(円/kWh)',
        'db_collection' : 'chugoku'
      },
      '四国' : {
        'column_name' : 'エリアプライス四国(円/kWh)',
        'db_collection' : 'shikoku'
      },
      '九州' : {
        'column_name' : 'エリアプライス九州(円/kWh)',
        'db_collection' : 'kyushu'
      },
    }

    # 必要なカラムのみ取得
    columns = ['受渡日', '時刻コード'] + [area_map[key]['column_name'] for key in area_map]

    df = pl.read_csv(io.StringIO(response.content.decode('utf-8')), columns=columns)

    # "受渡日"列が翌日以外の行を削除する
    tomorrow = tomorrow.strftime('%Y/%m/%d')
    df = df.filter(pl.col('受渡日') == tomorrow)

    # レコード0件の場合は終了
    if len(df) == 0:
      log("INFO", "no tomorrow data")
      return jsonify({'message': 'no tomorrow data'})

    # 時刻コードを00:00~23:30に変換
    def convert_timecode(timecode: int) -> str:
        hour = str((timecode -1)  // 2).zfill(2)
        minute = "00" if (timecode -1) % 2 == 0 else "30"
        return f"{hour}:{minute}"

    df = df.with_columns(
        pl.col("時刻コード").apply(lambda x: convert_timecode(x)).alias("time")
    )

    # エリアごとに{時間 : エリアプライス}を作成してdictに追加
    data_dict = {}
    for area in area_map:
        data_dict[area_map[area]['db_collection']] = dict(zip(df['time'], df[area_map[area]['column_name']]))

    # 環境変数からプロジェクトID取得
    project_id = os.environ.get('PROJECT_ID')

    # dictを一括で保存する。サブジェクトは"yyyy-MM-dd"
    date = tomorrow.replace('/', '-')
    db = firestore.Client(project=project_id)
    doc_ref = db.collection('electricity_market_price').document(date)
    doc_ref.set(data_dict, merge=True)

    # 配信用ファイルをcloud storageに配置（ファイル形式はmessagepack）
    store_messagepack_file(data_dict, tomorrow)

    log("INFO", "success")
    return jsonify({'message': 'success'})

  except Exception as e:
    log("error", f"Error: {e}")
    return f"Error: {e}" 
  
def log(severity, message):
  print(json.dumps(dict(severity=severity, message=message)))

def store_messagepack_file(data, file_name):
  try:
    # 辞書型データをMessagePack形式に変換
    messagepack_data = msgpack.packb(data)

    # Cloud Storageクライアントを作成
    client = storage.Client()

    # バケットを取得
    bucket_name = os.environ.get('STORAGE_BUCKET_NAME')
    bucket = client.get_bucket(bucket_name)

    # MessagePackファイルをバケットにアップロード
    blob = bucket.blob(file_name+"/price.msgpack")
    blob.upload_from_string(messagepack_data, content_type='application/x-msgpack')

  except Exception as e:
    log("error", "配信ファイルの配置に失敗しました。")
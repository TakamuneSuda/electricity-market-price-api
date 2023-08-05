from google.cloud import firestore
from fastapi import HTTPException

db = firestore.Client()

def get_data_from_firestore(date: str, area: str) -> dict:
    """
    Firestoreから指定された日付とエリアの電力市場価格データを取得する関数。

    Parameters:
    -----------
    date : str
        取得する電力市場価格データの日付（yyyy-mm-dd形式の文字列）。
    area : str
        取得する電力市場価格データのエリア。指定しない場合はNone。

    Returns:
    --------
    data : Dict[str, Dict[str, float]]
        指定された日付とエリアの電力市場価格データが格納された辞書型オブジェクト。
        キーはエリア名、値は時刻とエリアの価格が格納された辞書型オブジェクト。
    """
    # Firestoreからデータの取得
    doc_ref = db.collection('electricity_market_price').document(date)
    doc = doc_ref.get()

    # 指定した日付のデータが取得できなかった場合
    if not doc.exists:
        error_type = f"NO_DATA_ON_DATE"
        error_message = f"No data found for the specified date.'{date}'."
        raise HTTPException(
            status_code=404,
            detail={'type':error_type, 'message':error_message}
        )

    # FirestoreのDocumentSnapshotクラスをdict型に変換
    data = doc.to_dict()

    # エリアが指定されている場合、日付で絞り込んだデータをエリアで絞り込む
    if area:
        area_list = area.split(',')
        selected_data = {}
        for request_area in area_list:
            if request_area not in data:
                error_type = f"NO_AREA_DATA_ON_DATE"
                error_message = f"No data found for the specified area '{request_area}' on date '{date}'."
                raise HTTPException(
                    status_code=404,
                    detail={'type':error_type, 'message':error_message}
                )
            selected_data[request_area] = data[request_area]
        data = selected_data

    return data

from agraffe import Agraffe, Service
from fastapi import FastAPI, HTTPException
import datetime
from model import get_data_from_firestore

app = FastAPI()

@app.get("/api/electricity_market_price")
async def get_electricity_market_price(date: str = None, area: str = None):
    # # パラメータのバリデーション
    validate_params(date, area)

    # # Firestoreからデータの取得
    data = get_data_from_firestore(date, area)

    # 正常なレスポンスの返却
    return data

# '/api/electricity_market_price'以外は404を返すように設定
@app.get("/{path}")
async def invalid_path(path: str):
    raise HTTPException(status_code=404, detail="Not Found")

# パラメータバリデーション
def validate_params(date: str, area: str) -> None:
    # 必須パラメータdateが指定されていない場合
    if date is None:
        error_type = f"MISSING_REQUIRED_PARAMETER_DATE"
        error_message = f"Date parameter is missing."
        raise HTTPException(
            status_code=400,
            detail={'type':error_type, 'message':error_message}
        )
    
    # パラメータdateのフォーマットがyyyy-mm-ddか確認
    try:
        datetime.datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        error_type = f"INVALID_DATE_FORMAT"
        error_message = f"Date parameter must be in the format of yyyy-mm-dd."
        raise HTTPException(
            status_code=400,
            detail={'type':error_type, 'message':error_message}
        )
    
    # パラメータareaが指定されている場合は、指定されたエリアが許容されているか確認
    if area:
        area_list = area.split(',')
        # 許容するエリア
        valid_areas = ['system', 'hokkaido', 'tohoku', 'tokyo', 'hokuriku', 'chubu', 'kansai', 'chugoku', 'shikoku', 'kyushu']
        if not all(area in valid_areas for area in area_list):
            error_type = f"INVALID_AREA_PARAMETER"
            error_message = f"Invalid area parameter. Allowed values are 'system', 'hokkaido', 'tohoku', 'tokyo', 'hokuriku', 'chubu', 'kansai', 'chugoku', 'shikoku', 'kyushu'"
            raise HTTPException(
                status_code=400,
                detail={'type':error_type, 'message':error_message}
            )
entry_point = Agraffe.entry_point(app)
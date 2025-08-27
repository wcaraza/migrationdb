from fastapi.testclient import TestClient
from app.main import app
import io


client = TestClient(app)

def test_metrics_employee_quarter_by_year():
    response = client.get("/metrics/employees-quarter-by-year?year=2021")
    assert response.status_code == 200

def test_upload_csv():
    csv_content = "id,department\n99,HR\n100,IT\n"
    file = io.BytesIO(csv_content.encode("utf-8"))

    response = client.post(
        "/migration/csv-from-dir",
        files={"files": ("departments.csv", file, "text/csv")},
    )

    assert response.status_code == 200
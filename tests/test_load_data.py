import pandas as pd
from load_data import load_pm25_data

def test_load_pm25_data(monkeypatch):
    def fake_download(year, url, gios_id, filename):
        return pd.DataFrame({"A": [1, 2, 3]})

    # Podmieniamy w module load_data
    monkeypatch.setattr("load_data.download_gios_archive", fake_download)

    years = [2015, 2018]
    gios_archive_url = "http://example.com/"
    gios_ids = {2015: "ID2015", 2018: "ID2018"}
    filenames = {2015: "file2015.xlsx", 2018: "file2018.xlsx"}

    result = load_pm25_data(years, gios_archive_url, gios_ids, filenames)

    #Klucze to lata
    assert list(result.keys()) == [2015, 2018]

    #Wartości to DataFrame
    assert isinstance(result[2015], pd.DataFrame)
    assert isinstance(result[2018], pd.DataFrame)

    # Sprawdzamy zawartość
    assert list(result[2015].columns) == ["A"]


from load_data import get_old_station_codes

def test_get_old_station_codes():
    data = {
        "Kod stacji": ["A", "B", "C"],
        "Stary Kod stacji": ["X", "Y, Z", None],
        "Miejscowość": ["Miasto1", "Miasto2", "Miasto3"]
    }
    df = pd.DataFrame(data)

    old_codes, cities = get_old_station_codes(df)

    assert old_codes == {"X": "A", "Y": "B", "Z": "B"}
    assert cities == {"A": "Miasto1", "B": "Miasto2", "C": "Miasto3"}


from load_data import clean_pm25_data

def test_clean_pm25_data_basic():
    dfs = {
        2018: pd.DataFrame([
            ["abc", 1, 2],  #wiersz do wyrzucenia
            ["Kod stacji", "X10", "X11"],
            ["2018-01-01 00:00:00", "2,5", "3,7"],
            ["2018-01-01 01:00:00", "4.0", "NaN"]
        ])
    }

    result = clean_pm25_data(dfs)
    cleaned = result[2018]

    #Filtrowanie - Powinny zostać 2 wiersze
    assert len(cleaned) == 2

    #Test nagłówków
    assert list(cleaned.columns) == ["Data", "X10", "X11"]

    #Test datetime
    assert pd.api.types.is_datetime64_any_dtype(cleaned["Data"])

    #Test konwersji przecinków do float
    assert cleaned["X10"].dtype == float
    assert cleaned["X11"].dtype == float

    #Test wartości (po przecinku)
    assert cleaned.iloc[0]["X10"] == 2.5
    assert cleaned.iloc[0]["X11"] == 3.7

from load_data import replace_old_codes

def test_replace_old_codes_basic():
    dfs = {
        2018: pd.DataFrame({
            "Data": ["t1", "t2"],
            "A1": [10.1, 11.1],
            "B2": [20.2, 21.2],
            "Z3": [99.9, 98.8],
        }),
        2019: pd.DataFrame({
            "Data": ["x1"],
            "B2": [30.3],
            "C3": [40.4],
        })
    }

    old_codes = {
        "A1": "NEW_A",
        "B2": "NEW_B",
    }

    result = replace_old_codes(dfs, old_codes)

    changed_2018 = result[2018]
    assert list(changed_2018.columns) == ["Data", "NEW_A", "NEW_B", "Z3"]
    
    # wartości  identyczne
    assert changed_2018["NEW_A"].tolist() == [10.1, 11.1]
    assert changed_2018["NEW_B"].tolist() == [20.2, 21.2]
    assert changed_2018["Z3"].tolist() == [99.9, 98.8]

    changed_2019 = result[2019]
    assert list(changed_2019.columns) == ["Data", "NEW_B", "C3"]
    # wartości bez zmian
    assert changed_2019["NEW_B"].tolist() == [30.3]
    assert changed_2019["C3"].tolist() == [40.4]


def test_replace_old_codes_empty_old_codes():
    dfs = {2021: pd.DataFrame({"Data": ["2024-01-01 00:00:00"], "A1": [10]})}

    old_codes = {}

    result = replace_old_codes(dfs, old_codes)
    changed = result[2021]

    # kolumny powinny być identyczne
    assert list(changed.columns) == ["Data", "A1"]

def test_replace_old_codes_no_changes():
    dfs = {
        2021: pd.DataFrame(columns=["Data", "A", "B"])
    }

    old_codes = {
        "X": "Y"
    }

    result = replace_old_codes(dfs, old_codes)
    changed = result[2021]

    assert list(changed.columns) == ["Data", "A", "B"]

from load_data import correct_dates

def test_correct_dates():
    dfs = {
        2019: pd.DataFrame({"Data": pd.to_datetime(["2019-07-01 00:00:10", "2020-01-01 00:00:00"])}),
        2020: pd.DataFrame({"Data": pd.to_datetime(["2020-05-01 01:00:00"])})
    }

    result = correct_dates(dfs)

    # 2019 - korekta(przesunięcie po północy)
    assert result[2019]["Data"].iloc[0] == pd.Timestamp("2019-06-30 23:59:59")
    # równo północ
    assert result[2019]["Data"].iloc[1] == pd.Timestamp("2019-12-31 23:59:59")

    changed = result[2019]
    assert pd.api.types.is_datetime64_any_dtype(changed["Data"])

    # 2020 - bez zmian
    assert result[2020]["Data"].iloc[0] == pd.Timestamp("2020-05-01 01:00:00")

from load_data import merge_dataframes
                                             
def test_merge_dataframes_basic():
    dfs = {2019: pd.DataFrame({"Data": pd.to_datetime(["2019-01-01 00:00:00", "2019-01-02 00:00:00"]), "X1": [10.0, 11.0], "X2": [20.0, 21.0], "X3": [20.0, 21.0]}),
        2020: pd.DataFrame({"Data": pd.to_datetime(["2020-01-01 00:00:00", "2020-01-02 00:00:00"]), "X1": [12.0, 13.0], "X2": [22.0, 23.0],}) }

    cities = {"X1": "Warszawa", "X3": "Kraków"}
    merged = merge_dataframes(dfs, cities)

    #concat po wierszach - 4 rekordy
    assert len(merged) == 4

    #MultiIndex kolumn(X3 wypada)
    expected_cols = pd.MultiIndex.from_tuples([
        ("Data", ""),
        ("Warszawa", "X1"),
        ("Nieznana", "X2")
    ])

    assert list(merged.columns) == list(expected_cols)

    #Konwersja do numeric
    assert merged[("Warszawa", "X1")].dtype == float
    assert merged[("Nieznana", "X2")].dtype == float

    #Kolumna Data pozostała datetime
    assert pd.api.types.is_datetime64_any_dtype(merged[("Data", "")])


from load_data import get_cities_years

def test_get_cities_years():
    df = pd.DataFrame({
        "Miasto1": [10, 20, 30],
        "Miasto2": [5,  15, 25],
        "Miasto3": [1,  2,  3],
    }, index=[2019, 2020, 2021])

    cities = ["Miasto1", "Miasto3"]
    years = [2020, 2021]

    result = get_cities_years(df, cities, years)

    #Sprawdzanie rozmiaru
    assert list(result["index"]) == [2020, 2021]
    assert list(result["Miasto1"]) == [20, 30]
    assert list(result["Miasto3"]) == [2, 3]

    #Sprawdzanie kolumn
    assert list(result.columns) == ["index", "Miasto1", "Miasto3"]


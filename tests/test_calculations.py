import pandas as pd

from calculations import calculate_station_monthly_averages
def test_calculate_station_monthly_averages():
    df = pd.DataFrame({
        ("Data", ""): pd.to_datetime(["2020-01-01", "2020-01-02", "2020-02-01", "2020-02-02"]), 
        ("Wrocław", "DsWrocAlWisn"): [10, 20, 30, 40],
        ("Łódź", "LdLodzCzerni"): [5, 15, 25, 35],
    })

    df.columns = pd.MultiIndex.from_tuples(df.columns)


    result = calculate_station_monthly_averages(df)
    assert (2020, 1) in result.index
    assert (2020, 2) in result.index

    assert result.loc[(2020, 1), ("Wrocław", "DsWrocAlWisn")] == 15
    assert result.loc[(2020, 1), ("Łódź", "LdLodzCzerni")] == 10
    assert result.loc[(2020, 2), ("Wrocław", "DsWrocAlWisn")] == 35
    assert result.loc[(2020, 2), ("Łódź", "LdLodzCzerni")] == 30


from calculations import calculate_city_monthly_averages
def test_calculate_city_monthly_averages_simple():
    index = pd.MultiIndex.from_tuples([
        (2015, 1),
        (2015, 1),
        (2015, 2),
        (2015, 2),
    ], names=["Rok", "Miesiąc"])

    columns = pd.MultiIndex.from_tuples([
        ("Dolnośląskie","Wrocław", "stacja1"),
        ("Dolnośląskie","Wrocław", "stacja2"),
        ("Małopolskie","Kraków", "stacja1"),
        ("Małopolskie","Kraków", "stacja2"),
    ],names=["Wojewodztwo", "Miejscowosc", "Stacja"])

    df = pd.DataFrame([
        [10, 20, 30, 40],
        [30, 50, 70, 90],
        [20, 40, 60, 80],
        [40, 80, 120, 160]
    ], index=index, columns=columns)

    result = calculate_city_monthly_averages(df)
    expected = pd.DataFrame(
        [
            [ (10+20)/2, (30+40)/2 ],
            [ (30+50)/2, (70+90)/2 ],
            [ (20+40)/2, (60+80)/2 ],
            [ (40+80)/2, (120+160)/2 ],
        ],
        index=index,
        columns=pd.Index(["Wrocław","Kraków"],name="Miejscowosc")
    )

    pd.testing.assert_frame_equal(result, expected, check_like=True)#check_like=True ignoruje kolejność

from calculations import calculate_days_exceeding_limit
def test_calculate_days_exceeding_limit():
    data = {
        ("Data", ""): pd.to_datetime([
            "2023-01-01", "2023-01-01", "2023-01-02", "2023-01-02"
        ]),
        ("Kraków", "KR1"): [20, 22, 10, 5],
        ("Warszawa", "WAW1"): [5, 10, 20, 21],
    }

    df = pd.DataFrame(data)
    df.columns = pd.MultiIndex.from_tuples(df.columns)


    result = calculate_days_exceeding_limit(df, limit=15)

    assert isinstance(result, pd.DataFrame)
    assert result.loc[2023, ("Kraków", "KR1")] == 1
    assert result.loc[2023, ("Warszawa", "WAW1")] == 1


from calculations import get_3_lowest_highest

def test_get_3_lowest_highest():
    df = pd.DataFrame({
    ("Miasto", "Kod_stacji_1"): [5, 10, 20],
    ("Miasto", "Kod_stacji_2"): [1, 2, 3],
    ("Miasto", "Kod_stacji_3"): [7, 50, 60],
    ("Miasto", "Kod_stacji_4"): [9, 8, 7],
    ("Miasto", "Kod_stacji_5"): [4, 3, 2],
    ("Miasto", "Kod_stacji_6"): [100, 90, 80],
    ("Miasto", "Kod_stacji_7"): [10, 19, 15],
    }, index=[2018, 2019, 2020])

    df.columns = pd.MultiIndex.from_tuples(df.columns)


    result = get_3_lowest_highest(df, 2020)

    expected_cols = [
        ("Miasto", "Kod_stacji_5"),
        ("Miasto", "Kod_stacji_2"),
        ("Miasto", "Kod_stacji_4"),
        ("Miasto", "Kod_stacji_6"),
        ("Miasto", "Kod_stacji_3"),
        ("Miasto", "Kod_stacji_1"),
    ]

    assert list(result.columns) == expected_cols

    # sprawdzamy, że kształt to 6 kolumn
    assert result.shape[1] == 6

from calculations import calculate_days_exceeding_limit_by_province


def test_calculate_days_exceeding_limit_by_province():
    # Przygotowanie danych
    dates = pd.to_datetime([
        "2020-01-01 10:00",
        "2020-01-02 10:00",
        "2020-01-03 10:00",
    ])

    columns = pd.MultiIndex.from_tuples(
        [
            ("Mazowieckie", "Warszawa", "A"),
            ("Mazowieckie", "Radom", "B"),
            ("Małopolskie", "Kraków", "C"),
            ("Małopolskie", "Tarnów", "D"),
        ],
        names=["Wojewodztwo", "Miejscowosc", "Stacja"],
    )

    data = [
        [10, 12, 20, 10],  # dzień 1
        [16, 14, 14, 13],  # dzień 2
        [8, 9, 18, 17],    # dzień 3
    ]

    df = pd.DataFrame(data, index=dates, columns=columns)
    df["Data"] = df.index

    # Wywołanie funkcji
    result = calculate_days_exceeding_limit_by_province(df, limit=15)

    # Oczekiwany wynik
    expected = pd.DataFrame(
        [[1,2]],
        index=pd.Index([2020],name = "Data",dtype="int32"),
        columns = pd.Index([ "Mazowieckie","Małopolskie"], name="Wojewodztwo"),
    )

    print(result.columns)
    print(result.columns.names)
    print(result)
    print(expected.columns)
    print(expected.columns.names)
    print(expected)

    # Sprawdzenie
    pd.testing.assert_frame_equal(result, expected,check_like=True)#check_like=True ignoruje kolejność



import pandas as pd
import requests
import zipfile
import io, os
import re
from bs4 import BeautifulSoup
from io import BytesIO

'''
Moduł do wczytywania i czyszczenia danych
'''


# funkcja do ściągania podanego archiwum
def download_gios_archive(year, gios_archive_url, gios_id, filename):
    # Pobranie archiwum ZIP do pamięci
    url = f"{gios_archive_url}{gios_id}"
    response = requests.get(url)
    response.raise_for_status()  # jeśli błąd HTTP, zatrzymaj
    df = pd.DataFrame()
    
    # Otwórz zip w pamięci
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # znajdź właściwy plik z PM2.5
        if not filename:
            print(f"Błąd: nie znaleziono {filename}.")
        else:
            # wczytaj plik do pandas
            with z.open(filename) as f:
                try:
                    df = pd.read_excel(f, header=None)
                except Exception as e:
                    print(f"Błąd przy wczytywaniu {year}: {e}")
    return df

# funkcja do pobierania danych PM2.5 dla podanych lat
def load_pm25_data(years, gios_archive_url, gios_ids, filenames):
    data_frames = {} # słownik do przechowywania DataFrame dla każdego roku
    for year in years:
        print(f'Pobieranie danych PM2.5 dla roku {year}...')
        df = download_gios_archive(year, gios_archive_url, gios_ids[year], filenames[year])
        data_frames[year] = df
        print(f'Dane PM2.5 dla roku {year} pobrane. Kształt DataFrame: {df.shape}\n')

    return data_frames

# funkcja do wczytywania metadanych ze wskazanego pliku
def load_metadata():
    """
    Wyszukuje najnowszy plik metadanych GIOŚ na stronie archiwum,
    pobiera go i zwraca jako DataFrame.
    """
    
    archive_url = "https://powietrze.gios.gov.pl/pjp/archives"

    r = requests.get(archive_url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Linki z 'downloadFile/...'
    links = soup.find_all("a", href=True)
    candidates = []

    for a in links:
        href = a["href"]
        text = a.get_text(strip=True).lower()

        # warunek: tekst zawiera metadane itp.
        if "meta" in text and "downloadFile" in href:
            candidates.append((text, href))

    if not candidates:
        print("Nie znaleziono pliku metadanych!")
        return pd.DataFrame()

    text, href = candidates[0]

    file_url = "https://powietrze.gios.gov.pl" + href

    r = requests.get(file_url)
    r.raise_for_status()

    df = pd.read_excel(BytesIO(r.content), header=0)
    df = df.rename(columns={'Stary Kod stacji \n(o ile inny od aktualnego)': 'Stary Kod stacji'})
    
    return df
    
# Funkcja pomocnicza do wyciągania starych kodów stacji z metadanych
def get_old_station_codes(metadata_df):
    metadata_filtered = metadata_df[metadata_df["Stary Kod stacji"].notna()]
    old_codes = {}
    for k, row in metadata_filtered.iterrows():
        old = row['Stary Kod stacji']
        new = row['Kod stacji']
        if isinstance(old, str):
            for code in old.split(','): # w jednej komórce metadanych może być kilka starych kodów rozdzielonych przecinkiem
                old_codes[code.strip()] = new
    cities = dict(zip(metadata_df["Kod stacji"], metadata_df["Miejscowość"]))

    return old_codes, cities

# Funckja do czyszczenia DataFrame z danymi PM2.5
def clean_pm25_data(dfs):
    result_dfs = {}
    for year, df in dfs.items():
        cleaned_df = df.copy()
        date_format = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

        # Zostawiamy tylko wiersze z potrzebnymi danymi
        mask = (cleaned_df.iloc[:, 0].astype(str).str.match(date_format) |
                (cleaned_df.iloc[:, 0] == 'Kod stacji'))
        
        cleaned_df = cleaned_df[mask].reset_index(drop=True)

        # Ustawienie wiersza gdzie jest 'Kod stacji' jako nagłówki kolumn
        id = cleaned_df[cleaned_df.iloc[:, 0] == 'Kod stacji'].index[0]
        cleaned_df.columns = cleaned_df.loc[id].tolist()
        cleaned_df = cleaned_df.drop(index=id).reset_index(drop=True)

        # Przemianowanie kolumny z datami i zmiana na format datetime
        cleaned_df = cleaned_df.rename(columns={'Kod stacji': 'Data'})
        cleaned_df['Data'] = pd.to_datetime(cleaned_df['Data'])

        result_dfs[year] = cleaned_df
        print(f'Rok {year} - rozmiar po czyszczeniu: {cleaned_df.shape}')

    return result_dfs

# Funkcja do zamiany starych kodów stacji na nowe w DataFrame
def replace_old_codes(dfs, old_codes):
    result_dfs = {}
    for year, df in dfs.items():
        changed_df = df.copy()
        # print(changed_df)
        stations = changed_df.columns.tolist()
        changes = 0

        print(f'Rok {year} - sprawdzanie kodów stacji do zamiany...')

        for station in stations[1:]:
            if station in old_codes:
                new_code = old_codes[station]
                #print(f"Zamiana kodu stacji: {station} na {new_code}")
                stations[stations.index(station)] = new_code
                changes+=1

        print(f'Zmieniono {changes}')
        changed_df.columns = stations
        result_dfs[year] = changed_df

    return result_dfs

# Funkcja do korekty dat
def correct_dates(dfs):
    result_dfs = {}
    for year, df in dfs.items():
        changed_df = df.copy()
        print(f'Przed korektą daty - rok {year}:')
        print(changed_df['Data'].head(3))
        print('....')
        print(changed_df['Data'].tail(3))
        print('\n')

        changed_df['Data'] = changed_df['Data'].apply(
                lambda x: x - pd.Timedelta(seconds=1) if x.time() == pd.Timestamp("00:00:00").time() else x
            )
        print(f'Po korekcie daty - rok {year}:')
        print(changed_df['Data'].head(3))
        print('....')
        print(changed_df['Data'].tail(3))
        print('\n')

        result_dfs[year] = changed_df
        
    return result_dfs


# Funkcja do łączenia danych z różnych lat w jeden DataFrame
def merge_dataframes(dfs, cities):
    merged_df = pd.concat(dfs.values(), axis=0, join='inner', ignore_index=True)
    
    # Zamiana na MultiIndex
    new_columns = []
    for col in merged_df.columns:
        if col == "Data":
            new_columns.append(("Data", ""))  # np. zostaw "Data" jako kolumnę dat
        else:
            miejscowosc = cities.get(col, "Nieznana")  # default jeśli brak w metadanych
            new_columns.append((miejscowosc, col))

    merged_df.columns = pd.MultiIndex.from_tuples(new_columns)

    # Konwersja kolumn do odpowiednich typów
    cols_to_convert = merged_df.columns[1:]
    merged_df[cols_to_convert] = merged_df[cols_to_convert].apply(pd.to_numeric, errors="coerce")

    return merged_df

# Funkcja do zapisywania DataFrame do pliku Excel
def save_to_excel(df, output_path):
    try:
        df.to_excel(output_path)
        print(f'Dane zapisane do {output_path}')
    except Exception as e:
        print(f'Błąd przy zapisywaniu do pliku Excel: {e}')

def get_cities_years(df, cities, years):
    '''
    Docstring for get_cities_years
    
    :param df: Description
    :param cities: Description
    :param years: Description
    '''
    result_df = df.copy()
    result_df = result_df[cities]
    result_df = result_df.loc[years].reset_index()

    return result_df

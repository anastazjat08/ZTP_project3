import pandas as pd
import requests
import zipfile
import io
import re
from bs4 import BeautifulSoup
from io import BytesIO

'''
Moduł do wczytywania i czyszczenia danych
'''


def download_gios_archive(year, gios_archive_url, gios_id, filename):
    """ Ściąganie podanego archiwum GIOS i wczytanie pliku z danymi PM2.5 do DataFrame
    Args:
        year (int): rok
        gios_archive_url (str): URL do archiwum GIOS
        gios_id (str): ID archiwum GIOS
        filename (str): nazwa pliku do pobrania

    Returns:
        pd.DataFrame: dane PM2.5 dla podanego roku
    """
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


def load_pm25_data(years, gios_archive_url, gios_ids, filenames):
    """ Pobiera dane PM2.5 dla podanych lat z archiwum GIOS
    Args:
        years (list): lista lat do pobrania
        gios_archive_url (str): URL do archiwum GIOS
        gios_ids (dict): słownik z ID archiwów dla każdego roku
        filenames (dict): słownik z nazwami plików dla każdego roku
    Returns:
        dict: słownik z DataFrame dla każdego roku
    """
    data_frames = {} # słownik do przechowywania DataFrame dla każdego roku
    for year in years:
        df = download_gios_archive(year, gios_archive_url, gios_ids[year], filenames[year])
        data_frames[year] = df

    return data_frames


def load_metadata():
    """ Wyszukuje najnowszy plik metadanych GIOS na stronie archiwum,
        pobiera go i zwraca jako DataFrame.

    Returns:
        pd.DataFrame: dane metadanych GIOS
    """
    
    archive_url = "https://powietrze.gios.gov.pl/pjp/archives"

    try:
        r = requests.get(archive_url)
        r.raise_for_status()
    except Exception as e:
        print(f"Błąd pobierania strony archiwum: {e}")
        return None

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
        return None

    text, href = candidates[0]

    file_url = "https://powietrze.gios.gov.pl" + href

    try:
        r = requests.get(file_url)
        r.raise_for_status()
    except Exception as e:
        print(f"Błąd pobierania pliku metadanych: {e}")
        return None

    try:
        df = pd.read_excel(BytesIO(r.content), header=0)
        df = df.rename(columns={'Stary Kod stacji \n(o ile inny od aktualnego)': 'Stary Kod stacji'})
    except Exception as e:
        print(f"Błąd odczytu pliku metadanych: {e}")
        return None
    return df
    

def get_old_station_codes(metadata_df):
    """ Wyciąga stare kody stacji z metadanych

    Args:
        metadata_df (pd.DataFrame): dane metadanych GIOS

    Returns:
        tuple: słownik mapujący stare kody stacji na nowe
        oraz słownik mapujący kody stacji na nazwy miejscowości
    """
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


def clean_pm25_data(dfs):
    """Czyści Dataframe z danymi PM2.5

    Args:
        dfs (dict): słownik z DataFrame dla każdego roku

    Returns:
        dict: słownik z oczyszczonymi DataFrame dla każdego roku
    """
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

        # Zamiana przecinków na kropki (jeśli plik używa przecinków jako separatora dziesiętnego, np. 2018)
        cleaned_df = cleaned_df.replace(',', '.', regex=True).infer_objects(copy=False)

        # Konwersja kolumn stacji na typ float
        for col in cleaned_df.columns:
            if col != 'Data':
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')


        result_dfs[year] = cleaned_df

    return result_dfs


def replace_old_codes(dfs, old_codes):
    """Zamienia stare kody stacji na nowe w Dataframe

    Args:
        dfs (dict): słownik z DataFrame dla każdego roku
        old_codes (dict): słownik mapujący stare kody stacji na nowe

    Returns:
        dict: słownik z DataFrame z zamienionymi kodami stacji
    """
    result_dfs = {}
    for year, df in dfs.items():
        changed_df = df.copy()
        stations = changed_df.columns.tolist()
        changes = 0
        sample_changes = []

        for station in stations[1:]:
            if station in old_codes:
                new_code = old_codes[station]
                if len(sample_changes) < 5:
                    sample_changes.append((station, new_code))
                
                stations[stations.index(station)] = new_code
                changes += 1

        # sanity check
        print(f"\nRok {year} → liczba mapowań: {changes}")

        # kilka przykładowych mapowań
        if sample_changes:
            print("\nPrzykładowe mapowania:")
            for old, new in sample_changes:
                print(f"    {old} → {new}")

        changed_df.columns = stations
        result_dfs[year] = changed_df

    return result_dfs


def correct_dates(dfs):
    """Poprawia daty

    Args:
        dfs (dict): słownik z DataFrame dla każdego roku

    Returns:
        dict: słownik z DataFrame z poprawionymi datami
    """
    result_dfs = {}
    for year, df in dfs.items():
        changed_df = df.copy()


        cutoff = pd.Timedelta(seconds=59)
        mask_midnight = changed_df['Data'].dt.time <= (pd.Timestamp("00:00:00") + cutoff).time()

        if mask_midnight.any():
            # bierzemy pierwszy
            idx = mask_midnight.idxmax()#bierzemy true
            example_before = changed_df.loc[idx, 'Data']
        else:
            example_before = None

        # korekta
        changed_df.loc[mask_midnight, 'Data'] = (changed_df.loc[mask_midnight, 'Data'].dt.normalize() - pd.Timedelta(seconds=1))

        # po korekcie sanity check
        if example_before is not None:
            example_after = changed_df.loc[idx, 'Data']
            print(f"Przykład zmiany daty w {year}: {example_before} → {example_after}")
        else:
            print(f"Rok {year}: brak dat wymagających korekty\n")

 
        result_dfs[year] = changed_df
        
    return result_dfs


def merge_dataframes(dfs, cities):
    """Łączy dane z różnych lat w jeden Dataframe

    Args:
        dfs (dict): słownik z DataFrame dla każdego roku
        cities (dict): słownik mapujący kody stacji na nazwy miejscowości

    Returns:
        dict: słownik z DataFrame z poprawionymi datami
    """
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

def save_to_excel(df, output_path):
    """Zapisuje Dataframe do pliku excel

    Args:
        df (DataFrame): DataFrame do zapisania
        output_path (str): ścieżka do pliku wyjściowego
    """
    try:
        df.to_excel(output_path)
    except Exception as e:
        print(f'Błąd przy zapisywaniu do pliku Excel: {e}')

def get_cities_years(df, cities, years):
    """Zwraca Dataframe z danymi dla podanych miast i lat
    Args:
        df (pd.DataFrame): DataFrame z danymi PM2.5.
        cities (list): lista nazw miast do wybrania.
        years (list): lista lat do wybrania.
    Returns:
        pd.DataFrame: DataFrame z danymi dla podanych miast i lat.
    """
    result_df = df.copy()
    result_df = result_df[cities]
    result_df = result_df.loc[years].reset_index()

    return result_df

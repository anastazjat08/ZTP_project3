import pandas as pd

'''
Moduł do obliczeń
'''


def calculate_station_monthly_averages(df):
    """
    Oblicza miesięczne średnie wartości PM2.5 dla każdej stacji w każdym roku
    
    Args:
        df (pd.DataFrame): DataFrame z danymi PM2.5, gdzie kolumny to kody stacji, a indeks to daty.
        
    Returns:
        pd.DataFrame: DataFrame z miesięcznymi średnimi wartościami PM2.5.
    """
    df_copy = df.copy()
    months_means = (
        df_copy.groupby([df_copy["Data"].dt.year, df_copy["Data"].dt.month]).mean(numeric_only=True)
    )

    # Ustawienie nazw indeksów
    months_means.index.names = ["Rok", "Miesiąc"]

    return months_means

def calculate_city_monthly_averages(df):
    """
    Oblicza miesięczne średnie wartości PM2.5 dla każdego miasta w każdym roku
    
    Args:
        df (pd.DataFrame): DataFrame ze średnimi dla każdej stacji.
        
    Returns:
        pd.DataFrame: DataFrame z miesięcznymi średnimi wartościami PM2.5 dla miejscowości.
    """
    df_copy = df.copy()
    city_month_means = df_copy.T.groupby(level=0).mean().T

    return city_month_means


def calculate_days_exceeding_limit(df, limit=15):
    """
    Oblicza liczbę dni w miesiącu, kiedy średnia dzienna wartość PM2.5 przekracza określony limit.
    
    Args:
        df (pd.DataFrame): DataFrame z danymi PM2.5, gdzie kolumny to kody stacji, a indeks to daty.
        limit (float): Limit wartości PM2.5 do sprawdzenia przekroczeń.
        df (pd.DataFrame): DataFrame z danymi PM2.5, gdzie kolumny to kody stacji, a indeks to daty.
        limit (float): Limit wartości PM2.5 do sprawdzenia przekroczeń.
    """

    df_copy = df.copy()
    # Obliczanie średnich dziennych stężeń na stacje
    daily_means = (
        df_copy.groupby(df_copy["Data"].dt.floor("D")).mean(numeric_only=True)
    )

    # Sprawdzanie ile dni w każdym roku przekroczono 15 µg/m^3 dla każdej stacji
    exceeded = daily_means > limit
    result = exceeded.groupby(exceeded.index.year).sum()

    return result

def get_3_lowest_highest(df, year):
    """
    Znajduje 3 najniższych i 3 najwyższych miesięcznych średnich wartości PM2.5 dla każdej stacji.
    
    Args:
        df (pd.DataFrame): DataFrame z miesięcznymi średnimi wartościami PM2.5.
        year (int): Rok, dla którego obliczamy wartości.

    Returns:
        df (pd.DataFrame): DataFrame z 3 najniższymi i 3 najwyższymi wartościami PM2.5 dla każdej stacji.
    """
    exceed = df.loc[year].copy()
    smallest3 = exceed.nsmallest(3)
    largest3 = exceed.nlargest(3)

    top_bottom = list(smallest3.index) + list(largest3.index)
    df_results = df[top_bottom]

    return df_results
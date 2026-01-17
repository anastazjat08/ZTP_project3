import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

'''
Moduł do wizualizacji danych
'''

def plot_monthly_averages(df, title):
    """
    Tworzy wykres liniowy miesięcznych średnich wartości PM2.5
    
    Args:
        df (pd.DataFrame): DataFrame z miesięcznymi średnimi wartościami PM2.5.
        title (str): Tytuł wykresu.
    """
    cities = df.columns[2:].tolist()  # Pomijamy kolumny 'Rok' i 'Miesiąc'
    years = df['Rok'].unique()

    plt.figure(figsize=(10, 6))

    for city in cities:
        for year in years:
            dane = df[df["Rok"] == year]
            plt.plot(dane["Miesiąc"], dane[city], label=f"{city} {year}")

    plt.title(title)
    plt.xlabel("Miesiąc")
    plt.ylabel("PM2.5 [µg/m³]")
    plt.xticks(range(1, 13))
    plt.legend()
    plt.grid(True)

    plt.show()

def plot_heatmaps(df):
    """
    Tworzy mapę cieplną miesięcznych średnich wartości PM2.5 dla miast
    
    Args:
        df (pd.DataFrame): DataFrame z miesięcznymi średnimi wartościami PM2.5.
        title (str): Tytuł wykresu.
    """
    df_long = df.melt(id_vars=['Rok', 'Miesiąc'], var_name='miasto', value_name='PM25')

    # Facet dla każdego miasta
    g = sns.FacetGrid(df_long, col="miasto", col_wrap=4, height=4, sharey=True)
    g.map_dataframe(
        lambda data, color: sns.heatmap(
            data.pivot(index="Rok", columns="Miesiąc", values="PM25"), 
            cmap="Reds", 
            cbar=True,
            linewidths=0.5,
        )
    )
    g.set_titles(col_template="{col_name} - średnie miesięczne PM2.5")
    # Ustawienie etykiet
    for ax in g.axes.flat:
        ax.set_xlabel("Miesiąc")
        ax.set_ylabel("Rok")

        ax.tick_params(labelbottom=True, labelleft=True)

    plt.tight_layout()
    plt.show()

def plot_exceeding_days(df, title, x_label = "Stacje pomiarowe"):
    """Rysuje wykres słupkowy porównujący liczbę dni przekroczeń dla wybranych jednostek w różnych latach
    Args:
        df (pd.DataFrame): DataFrame z liczbą dni przekroczeń dla każdej jednostki (kolumna z danymi w df,
         reprezentujaca stacje, miejscowosc lub województwo) i roku.
        title (str): Tytuł wykresu.
        x_label (str): Nazwa osi poziomej.
    """

    df_plot = df.T
    # Rysowanie wykresu słupkowego porównującego liczbę dni przekroczeń
    # dla wybranych jednostek w różnych latach
    years = df_plot.columns
    stations = df_plot.index

    x = np.arange(len(stations))  # pozycje na osi X
    width = 0.2  # szerokość jednego słupka

    fig, ax = plt.subplots(figsize=(10,6))
    colors = plt.cm.Pastel2(np.linspace(0, 1, len(years)))

    # Rysowanie słupków dla każdego roku
    for i, year in enumerate(years):
        ax.bar(x + (i - 1)*width, df_plot[year], width, label=year, color=colors[i])  # (i - 1) żeby wyśrodkować

    ax.set_xticks(x)
    ax.set_xticklabels(stations, rotation=45, ha='right')
    ax.set_xlabel(x_label)
    ax.set_ylabel("Liczba dni z przekroczeniem (>15 µg/m³)")
    ax.set_title(title)
    ax.set_ylim(0, 365)
    ax.legend(title="Rok")

    plt.tight_layout()
    plt.show()
import matplotlib
matplotlib.use("Agg")
import pytest
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from unittest.mock import MagicMock, patch
from visualizations import plot_monthly_averages, plot_heatmaps, plot_exceeding_days


@pytest.fixture
def monthly_df():
    return pd.DataFrame({
        "Rok": [2020, 2020, 2021, 2021],
        "Miesiąc": [1, 2, 1, 2],
        "Kraków": [50, 40, 35, 25],
        "Warszawa": [45, 35, 30, 20],
    })

@pytest.fixture
def exceeding_df():
    return pd.DataFrame({
        "Kraków": [344, 343, 300],
        "Warszawa": [200, 210, 205],
    }, index=[2020, 2021, 2024])


def test_plot_monthly_averages_values(monthly_df):
    with patch("matplotlib.pyplot.plot") as mock_plot, patch("matplotlib.pyplot.show"):
        plot_monthly_averages(monthly_df, "Test")
        # powinny być linie dla każdej pary (city, year)
        expected_lines = len(monthly_df["Rok"].unique()) * (monthly_df.shape[1]-2)
        assert mock_plot.call_count == expected_lines

        # sprawdzamy faktyczne wartości danych
        for j, city in enumerate(monthly_df.columns[2:]):
            for i, year in enumerate(monthly_df["Rok"].unique()):
                idx = j * len(monthly_df["Rok"].unique()) + i
                x_vals, y_vals = mock_plot.call_args_list[idx][0]
                expected_x = monthly_df[monthly_df["Rok"] == year]["Miesiąc"].values
                expected_y = monthly_df[monthly_df["Rok"] == year][city].values
                assert np.array_equal(x_vals, expected_x)
                assert np.array_equal(y_vals, expected_y)



def test_plot_heatmaps(monthly_df):
    with patch("matplotlib.pyplot.show"):
        plot_heatmaps(monthly_df)

        fig = plt.gcf()
        axes = fig.get_axes()

        cities = monthly_df.columns[2:]

        # tylko osie z nazwami miast(heatmapy)
        city_axes = [ax for ax in axes if any(city in ax.get_title() for city in cities)]

        #Liczba paneli = liczba miast
        assert len(city_axes) == len(cities)

        # Sprawdzenie tytułów paneli zawierających nazwy miast(czy dobra kolejnośc)
        for ax, city in zip(city_axes, cities):
            assert city in ax.get_title()

        # Sprawdzenie etykiet osi x i y
        for ax in city_axes:
            assert ax.get_xlabel() == "Miesiąc"
            assert ax.get_ylabel() == "Rok"


def test_plot_heatmaps_values_with_patch(monthly_df):
    with patch("seaborn.heatmap") as mock_heatmap, patch("matplotlib.pyplot.show"):
        plot_heatmaps(monthly_df)

        # sprawdzamy argumenty macierzy przekazanej do heatmapy
        for i, city in enumerate(monthly_df.columns[2:]):
            call_args = mock_heatmap.call_args_list[i][0][0]
            expected = monthly_df.pivot(index='Rok', columns='Miesiąc', values=city)
            assert np.allclose(call_args.fillna(-1), expected.fillna(-1))


def test_plot_exceeding_days_values(exceeding_df):
    with patch("matplotlib.pyplot.subplots") as mock_subplots, patch("matplotlib.pyplot.show"):
        mock_ax = MagicMock()
        mock_subplots.return_value = (MagicMock(), mock_ax)
        plot_exceeding_days(exceeding_df, "Test")

        df_plot = exceeding_df.T
        years = df_plot.columns
        stations = df_plot.index

        # Sprawdzenie liczby wywołań bar (po jednym na rok)
        assert mock_ax.bar.call_count == len(years)

        # Sprawdzenie wysokości słupków dla każdego roku
        for i, year in enumerate(years):
            y_vals = mock_ax.bar.call_args_list[i][0][1]#wysokości słupków
            assert np.array_equal(y_vals, df_plot[year].values)

        # Sprawdzenie nazw stacji
        mock_ax.set_xticklabels.assert_called_once()
        labels = mock_ax.set_xticklabels.call_args[0][0]
        assert list(labels) == list(stations)
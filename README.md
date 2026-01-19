# Projekt: Analiza stężeń PM2.5 (2015, 2018, 2021, 2024)

## Opis projektu

Projekt analizuje dane dotyczące godzinnych stężeń pyłu PM2.5 w Polsce w latach 2015, 2018, 2021 i 2024.  

Zakres projektu obejmuje:
- pobieranie i programistyczne oczyszczanie danych z plików GIOS,
- ujednolicanie i aktualizację metadanych stacji pomiarowych,
- agregację miesięczną stężeń PM2.5 dla każdej stacji i roku,
- obliczanie liczby dni z przekroczeniem dobowej normy PM2.5 (15 µg/m³),
- porównania między miastami (Warszawa vs Katowice) na podstawie średnich miesięcznych wartości,
- wizualizacje danych w formie wykresów liniowych, heatmap i barplotów,
- testy,
- dokumentację.

Projekt został przygotowany w formie modułów `.py` oraz notebooka `.ipynb`.

---

## Struktura repozytorium

```
ZTP_project3/
├── load_data.py             # pobieranie, wczytywanie, czyszczenie i łączenie danych
├── calculations.py          # obliczenia i analiza statystyczna
├── visualizations.py        # rysowanie wykresów i wizualizacja wyników
├── projekt_1_ztp.ipynb      # główny notebook z analizą i opisami
├── combined_pm25_data.xlsx  # dane wyjściowe z notebooka
├── tests/                   # testy jednostkowe (pytest)
│   ├── test_load_data.py
│   └── test_calculations.py
├── .github/
│   └── workflows/
│       └── tests.yml        # pipeline CI (uruchamianie testów)
├── README.md                # dokumentacja projektu
└── opis_wkładu_ZTP_pr3.txt  # opis podziału pracy

```

---

## Wymagania

Wymagane biblioteki:

```
pandas
numpy
matplotlib
seaborn
requests
beautifulsoup4
openpyxl
pytest
```

---

## Uruchamianie

### 1. Instalacja zależności

```
pip install pandas numpy matplotlib seaborn requests beautifulsoup4 openpyxl pytest
```

### 2. Uruchomienie notebooka

Notebook, zawierający pełne wyniki, wykresy i opisy:

```
projekt_1_ztp.ipynb
```



---

## Testy pytest

Testy znajdują się w katalogu `tests/`.

Uruchomienie:

```
pytest -v
```

Testy obejmują:

- poprawność wczytywania danych (`test_load_data.py`)
- poprawność obliczeń (`test_calculations.py`)

---

## CI — Continuous Integration

Repozytorium zawiera plik umożliwiający automatyczne uruchamianie testów po dodaniu commitu.

Plik:

```
.github/workflows/tests.yml
```

Pipeline wykonuje uruchomienie `pytest`

---

## Release

Repozytorium zawiera release obejmujący działającą wersję kodu i notebooka.

---

## Dokumentacja

Dokumentacja obejmuje:

- opis w `README.md`
- docstringi w plikach `.py`
- notebook z opisami wyników

---

## Autorzy

Projekt wykonany w ramach realizacji Małego Projektu 3.  
Wkład zespołu został opisany w ramach pliku opis_wkładu_ZTP_pr3.txt


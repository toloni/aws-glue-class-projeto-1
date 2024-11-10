import pytest
from datetime import date

# Supondo que a classe DateToProcess esteja no arquivo 'date_to_process.py'
from utils.dates_to_process import DateToProcess


@pytest.fixture
def date_processor():
    # Fixture para instanciar a classe DateToProcess
    return DateToProcess()


def test_add_dates(date_processor):
    # Testar a adição de datas
    date_processor.add_dates("base1", 20241030, 20241020)
    dates = date_processor.get_dates("base1")
    assert len(dates) == 1
    assert dates[0] == (20241030, 20241020)


def test_get_unique_dates(date_processor):
    # Testar a obtenção de datas únicas
    date_processor.add_dates("base1", 20241030, 20241020)
    date_processor.add_dates("base1", 20241101, 20241021)
    unique_dates = date_processor.get_unique_dates()
    assert len(unique_dates) == 4
    assert 20241030 in unique_dates
    assert 20241020 in unique_dates
    assert 20241101 in unique_dates
    assert 20241021 in unique_dates


def test_get_last_date(date_processor):
    # Testar a obtenção da última data para uma base
    date_processor.add_dates("base1", 20241030, 20241020)
    date_processor.add_dates("base1", 20241101, 20241021)
    last_date = date_processor.get_last_date("base1")
    assert last_date == 20241101


def test_get_prev_date(date_processor):
    # Testar a obtenção da data anterior para uma base
    date_processor.add_dates("base1", 20241030, 20241020)
    date_processor.add_dates("base1", 20241101, 20241021)
    prev_date = date_processor.get_prev_date("base1")
    assert prev_date == 20241021


def test_get_dates_for_non_existent_base(date_processor):
    # Testar o retorno de uma base que não existe
    dates = date_processor.get_dates("base2")
    assert dates == []


def test_get_last_date_for_empty_base(date_processor):
    # Testar o retorno de None para última data quando a base não tem entradas
    last_date = date_processor.get_last_date("base2")
    assert last_date is None


def test_get_prev_date_for_empty_base(date_processor):
    # Testar o retorno de None para data anterior quando a base não tem entradas
    prev_date = date_processor.get_prev_date("base2")
    assert prev_date is None

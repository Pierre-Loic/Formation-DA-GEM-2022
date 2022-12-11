"""
Module de pré-traitement des données DVF en amont de la séparation train-test
"""

from datetime import date
import pandas as pd

def get_dvf_data(year: str = "2022", department: str = "38") -> pd.DataFrame:
    """
    Renvoie un dataframe des données des transactions immobilières d'une année pour un département
        Parameters:
                year (str): années des données
                department (str): numéro de département des données sur 2 chiffres (01, 02...)
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières
    """
    # A COMPLETER

def select_columns(house_dataframe: pd.DataFrame, selected_columns: tuple[str]) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les colonnes sélectionnées
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
                selected_columns (tuple[str]): tuple contenant les noms des colonnes du dataframe à sélectionner
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    # A COMPLETER

def select_type(house_dataframe: pd.DataFrame, house_types: tuple[str]) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les données des types d'habitations sélectionnées
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
                house_types (tuple[str]): tuple contenant les noms des types d'habitations à sélectionner
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    # A COMPLETER

def select_sales(house_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les données des ventes
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    # A COMPLETER

def drop_na(house_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Renvoie un dataframe des données sans données manquantes
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    # A COMPLETER

def check_na(house_dataframe: pd.DataFrame) -> None:
    """
    Renvoie le nombre de données manquantes
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                None
    """
    # A COMPLETER
        
def split_datetime(house_dataframe: pd.DataFrame) -> None:
    """
    Transforme la colonne "date_mutation" en objet "datetime" et crée deux colonnes
    pour le numéro du jour dans la semaine de l'achat et le nombre de jours passés depuis l'achat
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    # A COMPLETER

def drop_columns(house_dataframe: pd.DataFrame, columns_to_drop: list[str]) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les colonnes sélectionnées
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
                columns_to_drop (list[str]): liste contenant les noms des colonnes du dataframe à supprimer
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    # A COMPLETER

def to_csv(house_dataframe: pd.DataFrame, year: str, department: str) -> None:
    """
    Crée un fichier CSV des données du dataframe
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
                year (str): années des données
                department (str): numéro de département des données sur 2 chiffres (01, 02...)
        Returns:
                None
    """
    # A COMPLETER

#################
# Main function #
#################

def run(year: str = "2022", department: str = "38") -> pd.DataFrame:
    """
    Fait tourner tout le workflow de pré-traitement
        Parameters:
                year (str): années des données
                department (str): numéro de département des données sur 2 chiffres (01, 02...)
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    df = get_dvf_data(year, department)
    selected_columns = (
    "date_mutation", "valeur_fonciere", "surface_reelle_bati",
    "nombre_pieces_principales", "longitude", "latitude",
    "type_local", "nature_mutation",
    )
    df = select_columns(df, selected_columns)
    df = select_type(df, ("Maison", "Appartement"))
    df = select_sales(df)
    df = drop_na(df)
    check_na(df)
    df = split_datetime(df)
    df = drop_columns(df, ["date_mutation", "nature_mutation"])
    to_csv(df, year, department)
    return df
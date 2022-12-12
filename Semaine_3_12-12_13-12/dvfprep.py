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
    url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{department}.csv.gz"
    df = pd.read_csv(url, compression="gzip")
    return df

def select_columns(house_dataframe: pd.DataFrame, selected_columns: tuple[str]) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les colonnes sélectionnées
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
                selected_columns (tuple[str]): tuple contenant les noms des colonnes du dataframe à sélectionner
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    df = house_dataframe.copy()
    return df.loc[:,selected_columns]

def select_type(house_dataframe: pd.DataFrame, house_types: tuple[str]) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les données des types d'habitations sélectionnées
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
                house_types (tuple[str]): tuple contenant les noms des types d'habitations à sélectionner
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    df = house_dataframe.copy()
    return df[df["type_local"].isin(house_types)]

def select_sales(house_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les données des ventes
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    df = house_dataframe.copy()
    return df[df["nature_mutation"]=="Vente"]

def drop_na(house_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Renvoie un dataframe des données sans données manquantes
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    df = house_dataframe.copy()
    print(f"Nombre de lignes supprimées : {df.shape[0]-df.dropna().shape[0]}")
    return df.dropna()

def check_na(house_dataframe: pd.DataFrame) -> None:
    """
    Renvoie le nombre de données manquantes
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                None
    """
    df = house_dataframe.copy()
    if df.isna().sum().sum()==0:
        print("Il n'y a pas de données manquantes dans le dataframe")
    else:
        print("Nombre de données manquantes par colonne :")
        print(df.isna().sum())
        sns.heatmap(df.isna(), cbar=False)
        plt.show()
        
def split_datetime(house_dataframe: pd.DataFrame) -> None:
    """
    Transforme la colonne "date_mutation" en objet "datetime" et crée deux colonnes
    pour le numéro du jour dans la semaine de l'achat et le nombre de jours passés depuis l'achat
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    df = house_dataframe.copy()
    df["date_mutation"] = pd.to_datetime(df["date_mutation"])
    df["jour_semaine"] = df["date_mutation"].dt.dayofweek
    df["jours_depuis_achat"] = (pd.Timestamp.now()-df["date_mutation"]).dt.days
    return df

def drop_columns(house_dataframe: pd.DataFrame, columns_to_drop: list[str]) -> pd.DataFrame:
    """
    Renvoie un dataframe des données avec uniquement les colonnes sélectionnées
        Parameters:
                house_dataframe (pandas.DataFrame): dataframe des données immobilières
                columns_to_drop (list[str]): liste contenant les noms des colonnes du dataframe à supprimer
        Returns:
                df (pandas.DataFrame): dataframe des données immobilières traitées
    """
    df = house_dataframe.copy()
    return df.drop(columns_to_drop, axis=1)

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
    df = house_dataframe.copy()
    name = f"DVF_{year}_{department}_{str(date.today())}.csv"
    df.to_csv(name, index=False)

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
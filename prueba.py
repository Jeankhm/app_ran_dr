import pandas as pd

# Cargar el dataframe desde el archivo CSV
df = pd.read_csv("data/report_bta_el_ensueno_3m_20.csv")
print("Acá vemos el df inicial \n", df)
# Convertir la columna 'Timestamp' a tipo datetime si aún no lo está
df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%Y-%m-%d %H:%M:%S.%f")
# Obtener solo las columnas numéricas para calcular el promedio
numeric_columns = df.select_dtypes(include=["number"]).columns
# Agrupar por semana y tipo de celda, calculando promedios de columnas numéricas
df_grouped = (df.groupby([pd.Grouper(key="Timestamp", freq="W"), "Cell_name"])[numeric_columns]
    .mean()
    .reset_index()
)
# Mostrar el dataframe resultante
print("Acá vemos el df modificado por semana y promedio de columnas numéricas \n", df_grouped)

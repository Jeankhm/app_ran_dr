import pandas as pd
import psycopg2
import DBcredentials


def cell_query(start_date, end_date):
    # Función para consultar de la base de datos de celdas
    # Conectarse a la base de datos
    conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)
    # Crear un cursor
    cur = conn.cursor()
    # Realizar consulta a la base de datos PostgreSQL dentro del rango de fechas seleccionado
    cur.execute(
        """
        SELECT "Timestamp", "Cell_name", "L.Traffic.ActiveUser.DL.Avg"
        FROM "ran_1h_cell"
        WHERE DATE("Timestamp") BETWEEN %s AND %s
    """,
        (start_date, end_date),
    )
    print("Consulta exitosa")
    rows = cur.fetchall()  # Filas extraidas de la consulta
    columnas = ["Timestamp", "Cell_name", "L.Traffic.ActiveUser.DL.Avg"]
    df = pd.DataFrame(
        rows, columns=columnas
    )  # Creación de un dataframe a partir de la consulta
    df = df.sort_values(by="Timestamp")  # Ordeno por timestamp
    print("Dataframe creado exitosamente a partir de la consulta")
    return df


def calculos(df):
    df_final = df.copy()
    # Ejemplo de cálculos adicionales, asegurándose de que las columnas existen en el DataFrame
    if (
        "L.ChMeas.PRB.DL.Avail" in df_final.columns
        and "L.ChMeas.PRB.DL.Used.Avg" in df_final.columns
    ):
        df_final["DL_PRB_usage"] = (
            df_final["L.ChMeas.PRB.DL.Used.Avg"] / df_final["L.ChMeas.PRB.DL.Avail"]
        ) * 100
    if (
        "L.ChMeas.PRB.UL.Avail" in df_final.columns
        and "L.ChMeas.PRB.UL.Used.Avg" in df_final.columns
    ):
        df_final["UL_PRB_usage"] = (
            df_final["L.ChMeas.PRB.UL.Used.Avg"] / df_final["L.ChMeas.PRB.UL.Avail"]
        ) * 100
    if (
        "L.Thrp.bits.DL(bit)" in df_final.columns
        and "L.Thrp.bits.DL.LastTTI(bit)" in df_final.columns
        and "L.Thrp.Time.DL.RmvLastTTI(ms)" in df_final.columns
    ):
        df_final["User_Exp"] = (
            (df_final["L.Thrp.bits.DL(bit)"] - df_final["L.Thrp.bits.DL.LastTTI(bit)"])
            / (df_final["L.Thrp.Time.DL.RmvLastTTI(ms)"])
        ) / 1024
        df_final = df_final.drop(
            columns=["L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]
        )
    return df_final


def main():
    df = cell_query("2024-05-20", "2024-06-20")
    print("DataFrame original:\n", df)
    df.info(verbose=True)  # Muestra información del dataframe en consola
    reporte = calculos(df)
    print("Reporte:\n", reporte)
    reporte.info(verbose=True)
    reporte.to_csv(
        "Reporte_2024-05-20_2024-06-20.csv", index=False
    )  # Guardo el df como csv


if __name__ == "__main__":
    main()

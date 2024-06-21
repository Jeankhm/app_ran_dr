import pandas as pd
import psycopg2
import DBcredentials
# from datetime import datetime
# import datetime as dt


def cell_query(date): # Función para consultar de la base de datos de celdas
    # Conectarse a la base de datos
    conn = psycopg2.connect(**DBcredentials.BD_DATA_PARAMS)
    
    # Crear un cursor
    cur = conn.cursor()

    # Realizar consulta a la base de datos PostgreSQL dentro del rango de fechas seleccionado
    # cur.execute("""SELECT "Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Avg"
    #             FROM "ran_1h_cell" 
    #             WHERE DATE("Timestamp") BETWEEN %s AND %s""", (start_date, end_date,))

    # Realizar consulta a la base de datos PostgreSQL dentro del día seleccionado
    cur.execute("""SELECT "Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"
                FROM "ran_1h_cell" 
                WHERE DATE("Timestamp") = %s""", (date,))
    print("Consulta exitosa")
    rows = cur.fetchall() # Filas extraidas de la consulta
    columnas = ["Timestamp","Cell_name","L.Traffic.ActiveUser.DL.Avg","L.Traffic.ActiveUser.DL.Max","L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg","L.Thrp.bits.DL(bit)","L.Thrp.bits.UL(bit)","L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]
    df = pd.DataFrame(rows, columns=columnas) # Creación de un dataframe a partir de la consulta
    df = df.sort_values(by="Timestamp") # Ordeno por timestamp
    print("Dataframe creado exitosamente a partir de la consulta")

    return df

def calculos(df):
    df_final = df.copy()

    # Ocupación PRBs
    df_final["DL_PRB_usage"] = (df_final["L.ChMeas.PRB.DL.Used.Avg"] / df_final["L.ChMeas.PRB.DL.Avail"]) * 100 # Cálculo de % ocupación en downlink y guardado en nueva columna
    df_final["UL_PRB_usage"] = (df_final["L.ChMeas.PRB.UL.Used.Avg"] / df_final["L.ChMeas.PRB.UL.Avail"]) * 100 # # Cálculo de % ocupación en uplink y guardado en nueva columna
    df_final = df_final.drop(columns=["L.ChMeas.PRB.DL.Avail","L.ChMeas.PRB.DL.Used.Avg","L.ChMeas.PRB.UL.Avail","L.ChMeas.PRB.UL.Used.Avg"]) # Borro estas columnas
    print("Se ha añadido columnas de ocupación PRB DL y UL")

    # User experience
    df_final["User_Exp"] = ((df_final["L.Thrp.bits.DL(bit)"]-df_final["L.Thrp.bits.DL.LastTTI(bit)"]) / (df_final["L.Thrp.Time.DL.RmvLastTTI(ms)"])) / 1024 # Calculo user experience
    df_final = df_final.drop(columns=["L.Thrp.bits.DL.LastTTI(bit)", "L.Thrp.Time.DL.RmvLastTTI(ms)"]) # Borro estas columnas
    print("Se ha añadido columna de user experience")

    return df_final

# def filas_nulas(df)

def main():
    df = cell_query("2024-05-21")
    print("dataframe original:\n", df)
    df.info(verbose=True) # Muestra información del dataframe en consola

    reporte = calculos(df)
    print("reporte:\n", reporte)
    reporte.info(verbose=True)
    reporte.to_csv("Reporte_2024-05-21.csv", index=False) # Guardo el df como csv


if __name__ == "__main__":
    main()
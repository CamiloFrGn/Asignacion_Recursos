# -*- coding: utf-8 -*-
"""
Created on Tue Oct 13 09:22:14 2020

@author: jsdelgadoc
"""


import modulo_conn_sql as mcq
import numpy as np
import pandas as pd 
import datetime

def conectarSQL():
    conn = mcq.ConexionSQL()
    cursor = conn.getCursor()
    return cursor

def obtenerDesagregacion(pais, version):
    #Conectar con base sql y ejecutar consulta
    cursor = conectarSQL()
    try:
        cursor.execute("SELECT * FROM SCAC_AV7_DesagregacionPronosticoCiudadPlantaDiaTabla  WHERE Version = ? AND Pais = ? " , (version, pais) )
        #obtener nombre de columnas
        names = [ x[0] for x in cursor.description]
        
        #Reunir todos los resultado en rows
        rows = cursor.fetchall()
        resultadoSQL = []
            
        #Hacer un array con los resultados
        while rows:
            resultadoSQL.append(rows)
            if cursor.nextset():
                rows = cursor.fetchall()
            else:
                rows = None
                
        #Redimensionar el array para que quede en dos dimensiones
        resultadoSQL = np.array(resultadoSQL)
        resultadoSQL = np.reshape(resultadoSQL, (resultadoSQL.shape[1], resultadoSQL.shape[2]) )
    finally:
            if cursor is not None:
                cursor.close()
    return pd.DataFrame(resultadoSQL, columns = names)

def querySQL(query, parametros):
    #Conectar con base sql y ejecutar consulta
    cursor = conectarSQL()
    try:
        cursor.execute(query, parametros)
        #obtener nombre de columnas
        names = [ x[0] for x in cursor.description]
        
        #Reunir todos los resultado en rows
        rows = cursor.fetchall()
        resultadoSQL = []
            
        #Hacer un array con los resultados
        while rows:
            resultadoSQL.append(rows)
            if cursor.nextset():
                rows = cursor.fetchall()
            else:
                rows = None
                
        #Redimensionar el array para que quede en dos dimensiones
        resultadoSQL = np.array(resultadoSQL)
        resultadoSQL = np.reshape(resultadoSQL, (resultadoSQL.shape[1], resultadoSQL.shape[2]) )
    finally:
            if cursor is not None:
                cursor.close()
    return pd.DataFrame(resultadoSQL, columns = names)

def percentile75(g):
    return np.percentile(g, 75)
def percentile65(g):
    return np.percentile(g, 65)
def percentile50(g):
    return np.percentile(g, 50)

def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def generar_tabla_recursos(pais, version, factor_disponibilidad):
    #carga de datos para calculo de camiones (ventana horaria, ciclo, dropsize, desagregacion)
    df_desagregacion = querySQL("SELECT * FROM SCAC_AV7_DesagregacionPronosticoCiudadPlantaDiaTabla  WHERE Version = ? AND Pais = ? " , (version, pais))
    df_nombre_cluster = querySQL("SELECT * FROM SCAC_AT1_NombreCluster WHERE Pais = ? AND Activo = 1 " , (pais))
    df_ventana_horaria = querySQL("SELECT * FROM  SCAC_AV9_VentanaHoraria WHERE Pais = ?" , (pais))
    df_ciclo = querySQL("SELECT * FROM  AV37_Componentes_Ciclo_Malla_Turnos_Clientes_Tabla" , ())
    df_dropsize = querySQL("SELECT * FROM  SCAC_AV10_Dropsize" , ())

    df_calendario = querySQL("SELECT * FROM  SCAC_AT3_diashabilesfuente WHERE Pais = ? " , (pais))
    df_calendario.rename(columns={'Fecha de entrega':'FechaEntrega' }, inplace = True)
    
    #copio los df para no estar halando los datos del sql cada vez que se realizan pruebas
    desagregacion = df_desagregacion.copy()
    nombre_cluster = df_nombre_cluster.copy()
    ventana_horaria = df_ventana_horaria.copy()
    ciclo = df_ciclo.copy()
    dropsize = df_dropsize.copy()
    
    dropsize['Dropsize'] = dropsize['Dropsize'].astype(float)
    dropsize = dropsize.groupby('Planta Unica')['Dropsize'].mean().reset_index()
    
    nombre_cluster = pd.DataFrame( nombre_cluster[['Desc Cluster', 'Planta Unica']]).drop_duplicates()
    
    ventana_horaria = ventana_horaria.fillna(value=np.nan)
    ventana_horaria ['VentanaHoraria'] = ventana_horaria ['VentanaHoraria'].astype(float) 
    ciclo = ciclo.fillna(value=np.nan)
    
    desagregacion = pd.merge(desagregacion, nombre_cluster[['Planta Unica', 'Desc Cluster']], how='left', left_on='PlantaUnica', right_on='Planta Unica').drop('Planta Unica',1)
    desagregacion = desagregacion.groupby(['Pais', 'Desc Cluster', 'PlantaUnica', 'FechaEntrega', 'Version'])['M3Forecast'].sum().reset_index()
    
    
    desagregacion['M3Forecast'] = desagregacion['M3Forecast'].astype(float)
    ventana_horaria = ventana_horaria.groupby(['Nombre Centro'])['VentanaHoraria'].mean().reset_index()
    ciclo_pivot = pd.pivot_table(
        ciclo,
        index='Planta',
        values=['T.Cargue','T.Planta','T.Ida', 'T.Obra', 'T.Regreso'],
        aggfunc = np.mean
        )
    
    ciclo_pivot['ciclo_total'] = ciclo_pivot['T.Cargue'] + ciclo_pivot['T.Planta'] + ciclo_pivot['T.Ida'] + ciclo_pivot['T.Obra'] + ciclo_pivot['T.Regreso']
    ciclo_pivot = pd.DataFrame(ciclo_pivot[['ciclo_total']])
    matriz_recursos = pd.merge(desagregacion, ventana_horaria, how='left', left_on='PlantaUnica', right_on='Nombre Centro' ).drop("Nombre Centro", 1)
    matriz_recursos = pd.merge(matriz_recursos, ciclo_pivot, how = 'left', left_on='PlantaUnica', right_on='Planta' )
    matriz_recursos = pd.merge(matriz_recursos, dropsize, how = 'left', left_on='PlantaUnica', right_on='Planta Unica' ).drop('Planta Unica', 1)
    matriz_recursos  = matriz_recursos.fillna(matriz_recursos.mean())
    matriz_recursos['CamionesRodando'] = (matriz_recursos['M3Forecast'] * (matriz_recursos['ciclo_total']/60 )) / (matriz_recursos['VentanaHoraria'] * matriz_recursos['Dropsize'])
    
    matriz_recursos = pd.merge(matriz_recursos, df_calendario[['FechaEntrega', 'Semanas_mes']], on='FechaEntrega', how='inner')
    
    #resumen_camiones_rodando = matriz_recursos.groupby(['Desc Cluster','PlantaUnica', 'Semanas_mes']).agg({'VentanaHoraria':'mean', 'ciclo_total':'mean', 'Dropsize':'mean', 'M3Forecast': 'sum', 'CamionesRodando':[percentile(50), percentile(65), percentile(75)] })
    resumen_camiones_rodando = matriz_recursos.groupby(['Desc Cluster','PlantaUnica']).agg({'VentanaHoraria':'mean', 'ciclo_total':'mean', 'Dropsize':'mean', 'M3Forecast': 'sum', 'CamionesRodando':[percentile(75), percentile(80), percentile(90)] })
    resumen_camiones_rodando.columns = [' '.join(col).strip() for col in resumen_camiones_rodando.columns.values]
    resumen_camiones_rodando = resumen_camiones_rodando.reset_index()

    #matriz_recursos = pd.merge(matriz_recursos, resumen_camiones_rodando[['PlantaUnica', 'Semanas_mes', 'CamionesRodando percentile_50', 'CamionesRodando percentile_65', 'CamionesRodando percentile_75']], how = 'left', on = ['PlantaUnica', 'Semanas_mes'] )
    matriz_recursos = pd.merge(matriz_recursos, resumen_camiones_rodando[['PlantaUnica', 'CamionesRodando percentile_75', 'CamionesRodando percentile_80', 'CamionesRodando percentile_90']], how = 'left', on = ['PlantaUnica'] )
        
    return [resumen_camiones_rodando, matriz_recursos]

#pametros 
pais = 'Republica Dominicana'
version= 'CONSENSO_MAY_2022'
factor_disponibilidad = 0.75
"""
#carga de datos para calculo de camiones (ventana horaria, ciclo, dropsize, desagregacion)
df_desagregacion = querySQL("SELECT * FROM SCAC_AV7_DesagregacionPronosticoCiudadPlantaDiaTabla  WHERE Version = ? AND Pais = ? " , (version, pais))
df_nombre_cluster = querySQL("SELECT * FROM SCAC_AT1_NombreCluster WHERE Pais = ? AND Activo = 1 " , (pais))
df_ventana_horaria = querySQL("SELECT * FROM  SCAC_AV9_VentanaHoraria WHERE Pais = ?" , (pais))
df_ciclo = querySQL("SELECT * FROM  AV37_Componentes_Ciclo_Malla_Turnos_Clientes_Tabla" , ())
df_dropsize = querySQL("SELECT * FROM  SCAC_AV10_Dropsize" , ())

#copio los df para no estar halando los datos del sql cada vez que se realizan pruebas
desagregacion = df_desagregacion.copy()
nombre_cluster = df_nombre_cluster.copy()
ventana_horaria = df_ventana_horaria.copy()
ciclo = df_ciclo.copy()
dropsize = df_dropsize.copy()

dropsize['Dropsize'] = dropsize['Dropsize'].astype(float)
dropsize = dropsize.groupby('Planta Unica')['Dropsize'].mean().reset_index()

nombre_cluster = pd.DataFrame( nombre_cluster[['Desc Cluster', 'Planta Unica']]).drop_duplicates()

ventana_horaria = ventana_horaria.fillna(value=np.nan)
ventana_horaria ['VentanaHoraria'] = ventana_horaria ['VentanaHoraria'].astype(float) 
ciclo = ciclo.fillna(value=np.nan)

desagregacion = pd.merge(desagregacion, nombre_cluster[['Planta Unica', 'Desc Cluster']], how='left', left_on='PlantaUnica', right_on='Planta Unica').drop('Planta Unica',1)
desagregacion = desagregacion.groupby(['Pais', 'Desc Cluster', 'PlantaUnica', 'FechaEntrega', 'Version'])['M3Forecast'].sum().reset_index()


desagregacion['M3Forecast'] = desagregacion['M3Forecast'].astype(float)
ventana_horaria = ventana_horaria.groupby(['Nombre Centro'])['VentanaHoraria'].mean().reset_index()
ciclo_pivot = pd.pivot_table(
    ciclo,
    index='Planta',
    values=['T.Cargue','T.Planta','T.Ida', 'T.Obra', 'T.Regreso'],
    aggfunc = np.mean
    )

ciclo_pivot['ciclo_total'] = ciclo_pivot['T.Cargue'] + ciclo_pivot['T.Planta'] + ciclo_pivot['T.Ida'] + ciclo_pivot['T.Obra'] + ciclo_pivot['T.Regreso']
ciclo_pivot = pd.DataFrame(ciclo_pivot[['ciclo_total']])
matriz_recursos = pd.merge(desagregacion, ventana_horaria, how='left', left_on='PlantaUnica', right_on='Nombre Centro' ).drop("Nombre Centro", 1)
matriz_recursos = pd.merge(matriz_recursos, ciclo_pivot, how = 'left', left_on='PlantaUnica', right_on='Planta' )
matriz_recursos = pd.merge(matriz_recursos, dropsize, how = 'left', left_on='PlantaUnica', right_on='Planta Unica' ).drop('Planta Unica', 1)
matriz_recursos  = matriz_recursos.fillna(matriz_recursos.mean())
matriz_recursos['CamionesRodando'] = (matriz_recursos['M3Forecast'] * (matriz_recursos['ciclo_total']/60 )) / (matriz_recursos['VentanaHoraria'] * matriz_recursos['Dropsize'])

resumen_camiones_rodando = pd.pivot_table(
    matriz_recursos,
    index=['Desc Cluster','PlantaUnica'],
    values=[ 'M3Forecast', 'CamionesRodando'],
    aggfunc =  {'M3Forecast': 'sum', 'CamionesRodando' : percentile75}
    )
resumen_camiones_rodando['M3Forecast'] = resumen_camiones_rodando['M3Forecast'].round() 
resumen_camiones_rodando['CamionesRodando'] = resumen_camiones_rodando['CamionesRodando'].round() 
resumen_camiones_rodando['CamionesAsignados'] =( resumen_camiones_rodando['CamionesRodando'] / factor_disponibilidad).round()
resumen_camiones_rodando['ProductividadOperativa'] = resumen_camiones_rodando['M3Forecast'] / resumen_camiones_rodando['CamionesRodando']
resumen_camiones_rodando['ProductividadOperativa'] = resumen_camiones_rodando['ProductividadOperativa'].round()



writer = pd.ExcelWriter("../datos/AsignacionRecuros_" + pais + version + pd.to_datetime("now").strftime("%Y-%m-%d-%H-%M-%S")  + ".xlsx", engine='xlsxwriter')
        
matriz_recursos.to_excel( writer, sheet_name="matriz_recursos" )
resumen_camiones_rodando.to_excel( writer, sheet_name="resumen_camiones_rodando" )

writer.save()
"""

res = generar_tabla_recursos(pais, version, factor_disponibilidad)
writer = pd.ExcelWriter("../datos/AsignacionRecuros_" + pais + version + pd.to_datetime("now").strftime("%Y-%m-%d-%H-%M-%S")  + ".xlsx", engine='xlsxwriter')

res[0].to_excel( writer, sheet_name="resumen" )        
res[1].to_excel( writer, sheet_name="matriz_recursos" )

writer.save()

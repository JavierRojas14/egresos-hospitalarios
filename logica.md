# Análisis de Egresos Hospitalarios

En este análisis se quiere ver la cantidad de egresos hospitalarios para diversos problemas
médicos, tanto cubiertos por GES y los no GES.

Las preguntas que se quieren responder con este análisis son:

- Cuántos casos del problema médico hubieron en cada año a nivel nacional?
- En qué región fué dónde hubo el mayor caso de estos problemas médicos?
- Cómo es la distribución etárea del problema médico?
- Cómo es la distribución por sexo del problema médico?
- Cómo es la distribución por tipo de previsión (FONASA e ISAPRE) del problema médico?
- Cómo es la distribución por sector público/privado del problema de salud?
- Cómo es la distribución por hospitales del problemad de salud?


Ahora, a nivel de Instituto Nacional del Tórax:

- Cuál es la cantidad de Egresos Hospitalarios del problema de salud en el Tórax? Cuánto es este 
porcentaje de la cantidad Nacional? Cómo se compara el porcentaje del Tórax con respecto a 
los otros Hospitales (Somos referencia?)
- Cuál es el grupo etáreo que mayoritariamente llega al Tórax?
- De cuántas procedencias llegan pacientes al Hospital del Tórax?

# Bases de Datos

Ahora, para el análisis de los Egresos se necesita una base de datos que contenga la siguiente
información:

Hospital|Región|Comuna|Lugar de derivación (de donde viene el paciente)|Edad del paciente|Sexo del paciente|Previsión del Paciente|Diagnóstico
-|-|-|-|-|-|-|-

## Variables presentes

- ID_PACIENTE: 
- ESTABLECIMIENTO_SALUD:  
- GLOSA_ESTABLECIMIENTO_SALUD:  
- PERTENENCIA_ESTABLECIMIENTO_SALUD:  
- SEREMI:  
- SERVICIO_DE_SALUD:  
- SEXO:  
- FECHA_NACIMIENTO:  
- EDAD_CANT:  
- TIPO_EDAD:  
- EDAD_A_OS:  
- PUEBLO_ORIGINARIO:  
- PAIS_ORIGEN:  
- GLOSA_PAIS_ORIGEN:  
- COMUNA_RESIDENCIA:  
- GLOSA_COMUNA_RESIDENCIA:  
- REGION_RESIDENCIA:  
- GLOSA_REGION_RESIDENCIA:  
- PREVISION:  
- BENEFICIARIO:  
- MODALIDAD:  
- PROCEDENCIA:  
- ANO_EGRESO:  
- FECHA_EGRESO:  
- AREA_FUNCIONAL_EGRESO:  
- DIAS_ESTADA:  
- CONDICION_EGRESO: Es una variable binaria. Tiene solamente valores 1 y 2 (ints)
- DIAG1: Es el primer diagnóstico del paciente. Está en formato CIE-10, con un nivel de desglose hasta .X (Ej: O80.9). Carece del punto.
- GLOSA_DIAG1: Es el primer diagnóstico del paciente. Es la codificación del código CIE-10 en palabras.
- DIAG2:  
- GLOSA_DIAG2:  
- INTERV_Q:  
- CODIGO_INTERV_Q_PPAL:  
- GLOSA_INTERV_Q_PPAL:  
- PROCED:  
- CODIGO_PROCED_PPAL:  
- GLOSA_PROCED_PPAL

# Pasos del análisis

1. En primer lugar, se debe elegir el problema de salud a estudiar y filtrar la base de datos
2. Con la base filtrada, se debe obtener el largo del DataFrame (el indice 0 del df.shape). 
Con esto se obtendrá la cantidad de casos del problema médico a nivel nacional, y sin ninguna
desagregación
3. Para obtener la cantidad de casos por región, realizar un df.groupby(by='Región').count() o .sum().
Con esto, se obtendrán los datos de casos por región. Se puede hacer un gráfico de geomapa
4. Para obtener la cantidad de casos por edad, realizar un df.groupby(by='Edad').count() o .sum().
Con esto, se obtendrán los datos de casos por edad. Se puede hacer un gráfico de distribución.
5. Para obtener la cantidad de casos por sexo, realizar un df.groupby(by='sexo').count() o .sum().
Con esto, se obtendrán los datos de casos por sexo. Se puede hacer un gráfico de distribución.
6. Para obtener la cantidad de casos por prevision, realizar un df.groupby(by='prevision').count() o .sum().
Con esto, se obtendrán los datos de casos por prevision. Se puede hacer un gráfico de distribución.

7. Para realizar el análisis del Tórax, se debe filtrar el DataFrame solamente para el Tórax. Luego, se deben seguir los pasos 2 a 6.


## Análisis de integridad de Datos

La primera pregunta que se quiere responder respecto a las bases de datos es:

- ¿Qué tan confiables son las bases de datos obtenidas?

Y actualmente se tienen 2 bases de datos:

1. Resumen de estadísticos de Egresos DEIS 2001 - 2021
2. Base de datos formato largo de Egresos 2013 y 2019

El primer objetivo, es saber de dónde se obtienen estos datos. Luego, se quiere responder la
pregunta:

- ¿Ambas bases de datos contienen los mismos datos?

Esto se puede corroborar:

1. Verificando que se tienen la misma cantidad de egresos entre ambas bases de datos
2. Verificando que al agrupar por grandes causas, se obtienen las mismas distribuciones de casos

Si los dos pasos anteriores se cumplen, entonces se puede decir que ambas bases de datos 
son iguales. 



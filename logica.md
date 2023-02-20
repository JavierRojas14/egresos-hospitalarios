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

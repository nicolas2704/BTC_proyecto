import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine
import dash
from dash import dcc, html
from dotenv import load_dotenv
load_dotenv()
import os

nombre_db="proyecto"

# conectar a la base de datos
def conexion_a_base_datos(nombre_db):
    contraseña_pos = os.getenv("contraseña_pos")
    usuario_pos = os.getenv("usuario_pos")
    host_post = os.getenv("host_postgres")
    try:
        cadena_conexion=f"postgresql://{usuario_pos}:{contraseña_pos}@{host_post}:5432/{nombre_db}"
        engine=create_engine(cadena_conexion)
        print("conexion exitosa")
        return engine
    except Exception as ex:
        print(ex)

# llamar funciones
engine=conexion_a_base_datos(nombre_db)

# consultas SQL
def consultas_sql(engine):
    dataframe=pd.read_sql("bitcoin_precios_30", engine)
    return dataframe

diagrama_de_velas=consultas_sql(engine)

# crea el objeto de la aplicacion Dash
app = dash.Dash(__name__)

# visualizacion
def visualizar(diagrama_de_velas):
    fig = go.Figure(data=[go.Candlestick(
        x=diagrama_de_velas["fecha"], # Usar la columna de fecha creada por reset_index
        open=diagrama_de_velas["apertura"],
        high=diagrama_de_velas["maximo"],
        low=diagrama_de_velas["minimo"],
        close=diagrama_de_velas["cierre"]
    )])
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        xaxis_title="Fecha",
        yaxis_title="Precio (USD $)",
        title="Grafico de velas de Bitcoin de los ultimos 30 dias",
        plot_bgcolor='#030303',
        paper_bgcolor='#030303',
        font_color='#cfcdcd',
        title_font_color='#d1a72c',
        title_font_size=20 
    )
    return fig

# mostrar grafico
# 3. Definir el Layout (Diseño) de la aplicación Dash
# El layout define la estructura HTML del tablero.
app.layout = html.Div([
    html.H1(
        children='Dashboard Financiero con Dash', 
        style={'textAlign': 'center', 'color': '#d1a72c'}
    ),
    # El componente dcc.Graph toma la figura de Plotly y la renderiza en el navegador.
    dcc.Graph(
        id='bitcoin-candlestick-chart',
        figure=visualizar(diagrama_de_velas)
    )
])

# 4. Iniciar el servidor Dash
if __name__ == '__main__':
    # La aplicación se ejecutará en http://127.0.0.1:8050/
    app.run(debug=True)
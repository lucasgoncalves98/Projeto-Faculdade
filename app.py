# Importação das bibliotecas
from dash import Dash, html, dcc, callback_context
from sqlalchemy import create_engine
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

app = Dash(__name__)

# Conexão com banco de dados
engine = create_engine('mysql+pymysql://root:admin@localhost:3306/projetobigdata')

# Parte referente as tabelas e colunas do banco de dados

# Seleção dos dados de 2018
query2018 = """
    SELECT 
         act2018.data_acidente as data,
         concat(substring(act2018.hora_acidente,1,2), ':', substring(act2018.hora_acidente,3,2)) as hora,
         act2018.fase_dia as periodo,
         act2018.cond_meteorologica as clima,
         act2018.tp_acidente as tipo_acidente,
         act2018.tp_pavimento as pavimento,
         act2018.cond_pista as condicao_pista,
         act2018.lim_velocidade as limite_velocidade,
         act2018.tp_pista as tipo_via,
         act2018.qtde_envolvidos as quantitade_envolvidos,
         act2018.qtde_obitos as quantidade_obitos,
         vitimas18.genero as genero,
         vitimas18.faixa_idade as faixa_idade,
         vitimas18.tp_envolvido as tipo_envolvido,
         vitimas18.susp_alcool as bebida_alcoolica,
         local.municipio as cidade,
         act2018.uf_acidente as estado,
         veiculo.tipo_veiculo as tipo_veiculo,
         veiculo.qtde_veiculos as quantidade_veiculos_envolvidos
    FROM 
        acidentes_2018 as act2018
    left join 
        localidade_total as local on act2018.chv_localidade = local.chv_localidade
    left join
        vitimas_2018 as vitimas18 on act2018.num_acidente = vitimas18.num_acidente
    left join 
        tipoveiculo_total as veiculo on act2018.num_acidente = veiculo.num_acidente
    limit 100
"""

# Seleção dos dados de 2019
query2019 = """
    SELECT 
         act2019.data_acidente as data,
         concat(substring(act2019.hora_acidente,1,2), ':', substring(act2019.hora_acidente,3,2)) as hora,
         act2019.fase_dia as periodo,
         act2019.cond_meteorologica as clima,
         act2019.tp_acidente as tipo_acidente,
         act2019.tp_pavimento as pavimento,
         act2019.cond_pista as condicao_pista,
         act2019.lim_velocidade as limite_velocidade,
         act2019.tp_pista as tipo_via,
         act2019.qtde_envolvidos as quantitade_envolvidos,
         act2019.qtde_obitos as quantidade_obitos,
         vitimas19.genero as genero,
         vitimas19.faixa_idade as faixa_idade,
         vitimas19.tp_envolvido as tipo_envolvido,
         vitimas19.susp_alcool as bebida_alcoolica,
         local.municipio as cidade,
         act2019.uf_acidente as estado,
         veiculo.tipo_veiculo as tipo_veiculo,
         veiculo.qtde_veiculos as quantidade_veiculos_envolvidos
    FROM 
        acidentes_2019 as act2019
    left join 
        localidade_total as local on act2019.chv_localidade = local.chv_localidade
    left join
        vitimas_2019 as vitimas19 on act2019.num_acidente = vitimas19.num_acidente
    left join 
        tipoveiculo_total as veiculo on act2019.num_acidente = veiculo.num_acidente
    limit 100
"""

#Seleção dos dados de 2020

query2020 = """
    SELECT 
         act2020.data_acidente as data,
         concat(substring(act2020.hora_acidente,1,2), ':', substring(act2020.hora_acidente,3,2)) as hora,
         act2020.fase_dia as periodo,
         act2020.cond_meteorologica as clima,
         act2020.tp_acidente as tipo_acidente,
         act2020.tp_pavimento as pavimento,
         act2020.cond_pista as condicao_pista,
         act2020.lim_velocidade as limite_velocidade,
         act2020.tp_pista as tipo_via,
         act2020.qtde_envolvidos as quantitade_envolvidos,
         act2020.qtde_obitos as quantidade_obitos,
         vitimas20.genero as genero,
         vitimas20.faixa_idade as faixa_idade,
         vitimas20.tp_envolvido as tipo_envolvido,
         vitimas20.susp_alcool as bebida_alcoolica,
         local.municipio as cidade,
         act2020.uf_acidente as estado,
         veiculo.tipo_veiculo as tipo_veiculo,
         veiculo.qtde_veiculos as quantidade_veiculos_envolvidos
              
    FROM 
        acidentes_2020 as act2020
                        
    left join 
        localidade_total as local on act2020.chv_localidade = local.chv_localidade
    left join
        vitimas_2020 as vitimas20 on act2020.num_acidente = vitimas20.num_acidente
    left join 
        tipoveiculo_total as veiculo on act2020.num_acidente = veiculo.num_acidente

        limit 100
       """

#Seleção dos dados de 2021

query2021 = """
    SELECT 
         act2021.data_acidente as data,
         concat(substring(act2021.hora_acidente,1,2), ':', substring(act2021.hora_acidente,3,2)) as hora,
         act2021.fase_dia as periodo,
         act2021.cond_meteorologica as clima,
         act2021.tp_acidente as tipo_acidente,
         act2021.tp_pavimento as pavimento,
         act2021.cond_pista as condicao_pista,
         act2021.lim_velocidade as limite_velocidade,
         act2021.tp_pista as tipo_via,
         act2021.qtde_envolvidos as quantitade_envolvidos,
         act2021.qtde_obitos as quantidade_obitos,
         vitimas21.genero as genero,
         vitimas21.faixa_idade as faixa_idade,
         vitimas21.tp_envolvido as tipo_envolvido,
         vitimas21.susp_alcool as bebida_alcoolica,
         local.municipio as cidade,
         act2021.uf_acidente as estado,
         veiculo.tipo_veiculo as tipo_veiculo,
         veiculo.qtde_veiculos as quantidade_veiculos_envolvidos
              
    FROM 
        acidentes_2021 as act2021
                        
    left join 
        localidade_total as local on act2021.chv_localidade = local.chv_localidade
    left join
        vitimas_2021 as vitimas21 on act2021.num_acidente = vitimas21.num_acidente
    left join 
        tipoveiculo_total as veiculo on act2021.num_acidente = veiculo.num_acidente

        limit 100
       """

#Seleção dos dados de 2022

query2022 = """
    SELECT 
         act2022.data_acidente as data,
         concat(substring(act2022.hora_acidente,1,2), ':', substring(act2022.hora_acidente,3,2)) as hora,
         act2022.fase_dia as periodo,
         act2022.cond_meteorologica as clima,
         act2022.tp_acidente as tipo_acidente,
         act2022.tp_pavimento as pavimento,
         act2022.cond_pista as condicao_pista,
         act2022.lim_velocidade as limite_velocidade,
         act2022.tp_pista as tipo_via,
         act2022.qtde_envolvidos as quantitade_envolvidos,
         act2022.qtde_obitos as quantidade_obitos,
         vitimas22.genero as genero,
         vitimas22.faixa_idade as faixa_idade,
         vitimas22.tp_envolvido as tipo_envolvido,
         vitimas22.susp_alcool as bebida_alcoolica,
         local.municipio as cidade,
         act2022.uf_acidente as estado,
         veiculo.tipo_veiculo as tipo_veiculo,
         veiculo.qtde_veiculos as quantidade_veiculos_envolvidos
              
    FROM 
        acidentes_2022 as act2022
                        
    left join 
        localidade_total as local on act2022.chv_localidade = local.chv_localidade
    left join
        vitimas_2022 as vitimas22 on act2022.num_acidente = vitimas22.num_acidente
    left join 
        tipoveiculo_total as veiculo on act2022.num_acidente = veiculo.num_acidente

        limit 100
       """

#Seleção dos dados de 2023

query2023 = """
    SELECT 
         act2023.data_acidente as data,
         concat(substring(act2023.hora_acidente,1,2), ':', substring(act2023.hora_acidente,3,2)) as hora,
         act2023.fase_dia as periodo,
         act2023.cond_meteorologica as clima,
         act2023.tp_acidente as tipo_acidente,
         act2023.tp_pavimento as pavimento,
         act2023.cond_pista as condicao_pista,
         act2023.lim_velocidade as limite_velocidade,
         act2023.tp_pista as tipo_via,
         act2023.qtde_envolvidos as quantitade_envolvidos,
         act2023.qtde_obitos as quantidade_obitos,
         vitimas23.genero as genero,
         vitimas23.faixa_idade as faixa_idade,
         vitimas23.tp_envolvido as tipo_envolvido,
         vitimas23.susp_alcool as bebida_alcoolica,
         local.municipio as cidade,
         act2023.uf_acidente as estado,
         veiculo.tipo_veiculo as tipo_veiculo,
         veiculo.qtde_veiculos as quantidade_veiculos_envolvidos
              
    FROM 
        acidentes_2023 as act2023
                        
    left join 
        localidade_total as local on act2023.chv_localidade = local.chv_localidade
    left join
        vitimas_2023 as vitimas23 on act2023.num_acidente = vitimas23.num_acidente
    left join 
        tipoveiculo_total as veiculo on act2023.num_acidente = veiculo.num_acidente

        limit 100
       """

# Dataframe para conexão com tabelas e colunas selecionada na query
df2018 = pd.read_sql_query(query2018, engine)
df2019 = pd.read_sql_query(query2019, engine)
df2020 = pd.read_sql_query(query2020, engine)
df2021 = pd.read_sql_query(query2021, engine)
df2022 = pd.read_sql_query(query2022, engine)
df2023 = pd.read_sql_query(query2023, engine)

# Contagem de registros por tabela
count_2018 = len(df2018)
count_2019 = len(df2019)
count_2020 = len(df2020)
count_2021 = len(df2021)
count_2022 = len(df2022)
count_2023 = len(df2023)

# Cálculo da quantidade total de registros
total_records = count_2018 + count_2019 + count_2020 + count_2021 + count_2022 + count_2023

# Parte referente ao gráfico 1
fig1 = px.bar(df2018, x="periodo", y="tipo_acidente", color="estado", barmode="group",
                 title="Relação entre causa do acidente e periodo do ocorrido")
fig2 = px.bar(df2019, x="periodo", y="tipo_acidente", color="estado", barmode="group",
                 title="Relação entre causa do acidente e periodo do ocorrido")
fig3 = px.bar(df2020, x="periodo", y="tipo_acidente", color="estado", barmode="group",
                 title="Relação entre causa do acidente e periodo do ocorrido")
fig4 = px.bar(df2021, x="periodo", y="tipo_acidente", color="estado", barmode="group",
                 title="Relação entre causa do acidente e periodo do ocorrido")
fig5 = px.bar(df2022, x="periodo", y="tipo_acidente", color="estado", barmode="group",
                 title="Relação entre causa do acidente e periodo do ocorrido")
fig6 = px.bar(df2023, x="periodo", y="tipo_acidente", color="estado", barmode="group",
                 title="Relação entre causa do acidente e periodo do ocorrido")

# Parte referente ao gráfico 2
fig7 = px.line(df2018, x="quantitade_envolvidos", y="quantidade_obitos", color="estado", symbol="estado",
                 title="Relação entre quantidade de envolvidos e quantidade de óbitos")
fig8 = px.line(df2019, x="quantitade_envolvidos", y="quantidade_obitos", color="estado", symbol="estado",
                 title="Relação entre quantidade de envolvidos e quantidade de óbitos")
fig9 = px.line(df2020, x="quantitade_envolvidos", y="quantidade_obitos", color="estado", symbol="estado",
                 title="Relação entre quantidade de envolvidos e quantidade de óbitos")
fig10 = px.line(df2021, x="quantitade_envolvidos", y="quantidade_obitos", color="estado", symbol="estado",
                 title="Relação entre quantidade de envolvidos e quantidade de óbitos")
fig11 = px.line(df2022, x="quantitade_envolvidos", y="quantidade_obitos", color="estado", symbol="estado",
                 title="Relação entre quantidade de envolvidos e quantidade de óbitos")
fig12 = px.line(df2023, x="quantitade_envolvidos", y="quantidade_obitos", color="estado", symbol="estado",
                 title="Relação entre quantidade de envolvidos e quantidade de óbitos")

# Parte referente ao gráfico 3
fig13 = px.bar(df2018, x="condicao_pista", y="pavimento", color="estado", barmode="group",
                 title="Relação entre condição da pista e o tipo de pavimento")
fig14 = px.bar(df2019, x="condicao_pista", y="pavimento", color="estado", barmode="group",
                 title="Relação entre condição da pista e o tipo de pavimento")
fig15 = px.bar(df2020, x="condicao_pista", y="pavimento", color="estado", barmode="group",
                 title="Relação entre condição da pista e o tipo de pavimento")
fig16 = px.bar(df2021, x="condicao_pista", y="pavimento", color="estado", barmode="group",
                 title="Relação entre condição da pista e o tipo de pavimento")
fig17 = px.bar(df2022, x="condicao_pista", y="pavimento", color="estado", barmode="group",
                 title="Relação entre condição da pista e o tipo de pavimento")
fig18 = px.bar(df2023, x="condicao_pista", y="pavimento", color="estado", barmode="group",
                 title="Relação entre condição da pista e o tipo de pavimento")

# Parte referente ao gráfico 4
fig19 = px.line(df2018, x="genero", y="faixa_idade", color="estado", symbol="estado",
                 title="Relação entre idade e gênero")
fig20 = px.line(df2019, x="genero", y="faixa_idade", color="estado", symbol="estado",
                 title="Relação entre idade e gênero")
fig21 = px.line(df2020, x="genero", y="faixa_idade", color="estado", symbol="estado",
                 title="Relação entre idade e gênero")  
fig22 = px.line(df2021, x="genero", y="faixa_idade", color="estado", symbol="estado",
                 title="Relação entre idade e gênero")  
fig23 = px.line(df2022, x="genero", y="faixa_idade", color="estado", symbol="estado",
                 title="Relação entre idade e gênero")  
fig24 = px.line(df2023, x="genero", y="faixa_idade", color="estado", symbol="estado",
                 title="Relação entre idade e gênero")        

# Callback para selecionar o gráfico
@app.callback(
     [Output('dashboard-graph', 'figure'),
     Output('line-graph', 'figure'),
     Output('scatter-graph', 'figure'),
     Output('bar-graph', 'figure'),
     Output('graph-title', 'children'),
     Output('record-counts', 'children')],
    [Input('button-2018', 'n_clicks'),
     Input('button-2019', 'n_clicks'),
     Input('button-2020', 'n_clicks'),
     Input('button-2021', 'n_clicks'),
     Input('button-2022', 'n_clicks'),
     Input('button-2023', 'n_clicks')]
)
def update_graph(bt2018, bt2019, bt2020, bt2021, bt2022, bt2023):
    ctx = callback_context

    # Verifica qual botão foi clicado
    if not ctx.triggered:
        return fig1, fig7, fig13, fig19, 'Gráfico de acidentes de trânsito de 2018', f'Quantidade de acidentes em 2018: {len(df2018)} registros'

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'button-2018':
        return fig1, fig7, fig13, fig19, 'Gráfico de acidentes de trânsito de 2018', f'Quantidade de acidentes em 2018: {len(df2018)} registros'
    elif button_id == 'button-2019':
        return fig2, fig8, fig14, fig20, 'Gráfico de acidentes de trânsito de 2019', f'Quantidade de acidentes em 2019: {len(df2019)} registros'
    elif button_id == 'button-2020':
        return fig3, fig9, fig15, fig21, 'Gráfico de acidentes de trânsito de 2020', f'Quantidade de acidentes em 2020: {len(df2020)} registros'
    elif button_id == 'button-2021':
        return fig4, fig10, fig16, fig22, 'Gráfico de acidentes de trânsito de 2021', f'Quantidade de acidentes em 2021: {len(df2021)} registros'
    elif button_id == 'button-2022':
        return fig5, fig11, fig17, fig23, 'Gráfico de acidentes de trânsito de 2022', f'Quantidade de acidentes em 2022: {len(df2022)} registros'
    elif button_id == 'button-2023':
        return fig6, fig12, fig18, fig24, 'Gráfico de acidentes de trânsito de 2023', f'Quantidade de acidentes em 2023: {len(df2023)} registros'

    # Gráfico padrão
    return fig1, fig7, fig13, fig19, 'Gráfico de acidentes de trânsito de 2018', f'Quantidade de acidentes em 2018: {len(df2018)} registros'

# Layout da aplicação
app.layout = html.Div(children=[
    html.H1(children='Acidentes de trânsito no Brasil'),

    html.H4(f'Quantidade de acidentes de trânsito ocorrido entre o período de 2018 à 2023: {total_records} registros'),  # Total de registros

    # Título dinâmico do gráfico
    html.H3(id='graph-title', children='Gráfico de acidentes de 2018'),

    # Gráficos 1
    dcc.Graph(
        id='dashboard-graph',  # ID único
        figure=fig1
    ),

    # Gráficos 2
    dcc.Graph(
        id='line-graph',  # ID único
        figure=fig7
    ),

    # Gráficos 3
    dcc.Graph(
        id='scatter-graph',  # ID único
        figure=fig13
    ),

    # Gráficos 4
    dcc.Graph(
        id='bar-graph',  # ID único
        figure=fig19
    ),

    # Botões para selecionar o gráfico
    html.Button('Exibir acidentes de 2018', id='button-2018', n_clicks=0),
    html.Button('Exibir acidentes de 2019', id='button-2019', n_clicks=0),
    html.Button('Exibir acidentes de 2020', id='button-2020', n_clicks=0),
    html.Button('Exibir acidentes de 2021', id='button-2021', n_clicks=0),
    html.Button('Exibir acidentes de 2022', id='button-2022', n_clicks=0),
    html.Button('Exibir acidentes de 2023', id='button-2023', n_clicks=0),
    
    # Seção para exibir contagem de registros por ano
    html.Div(id='record-counts', style={'margin-top': '20px', 'margin-bottom': '20px'})
])

if __name__ == '__main__':
    app.run(debug=True)

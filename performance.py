# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # mdb_to_df

# %%
import random
from datetime import datetime as dt
import numpy as np
import pyodbc
import pandas as pd


def mdb_to_df(file_name, sql):

    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        fr'DBQ={file_name};'
    )
    print(conn_str)
    cnxn = pyodbc.connect(conn_str)

    df = pd.read_sql(sql, cnxn)

    print(f'{file_name} Loaded')
    return df

# %% [markdown]
# # Cascade

# %%
# Determine alarms real periods


def cascade(df):

    df.reset_index(inplace=True, drop=True)
    df['TimeOffMax'] = df.TimeOff.cummax().shift()

    df.at[0, 'TimeOffMax'] = df.at[0, 'TimeOn']

    return df


# looping through turbines and applying cascade method
def apply_cascade(result_sum):

    # Sort by alarm ID
    result_sum.sort_values(['ID'], inplace=True)
    df = result_sum.groupby('StationId').apply(cascade)

    mask_root = (df.TimeOn.values >= df.TimeOffMax.values)
    mask_children = (df.TimeOn.values < df.TimeOffMax.values) & (
        df.TimeOff.values > df.TimeOffMax.values)
    mask_embedded = (df.TimeOff.values <= df.TimeOffMax.values)

    df.loc[mask_root, 'NewTimeOn'] = df.loc[mask_root, 'TimeOn']
    df.loc[mask_children, 'NewTimeOn'] = df.loc[mask_children, 'TimeOffMax']
    df.loc[mask_embedded, 'NewTimeOn'] = df.loc[mask_embedded, 'TimeOff']

    df.drop(columns=['TimeOffMax'], inplace=True)

    df.reset_index(inplace=True, drop=True)

    TimeOff = df.TimeOff
    NewTimeOn = df.NewTimeOn

    df['RealPeriod'] = abs(TimeOff - NewTimeOn)

    mask_siemens = (df['Error Type'] == 1)
    mask_tarec = (df['Error Type'] == 0)

    df['Period Siemens(s)'] = df[mask_siemens].RealPeriod  # .dt.seconds
    df['Period Tarec(s)'] = df[mask_tarec].RealPeriod  # .dt.seconds
    # df['RealPeriod'] = df['RealPeriod'].dt.seconds

    return df

# %% [markdown]
# # Read SUM


# %%


def read_sum(period):
    usecols_sum = """
    SELECT CDbl(TimeOn) AS TOn, CDbl(TimeOff) AS TOff,
    StationNr, Alarmcode, ID, Parameter
    FROM tblAlarmLog WHERE TimeOff IS NOT NULL
    union
    SELECT CDbl(TimeOn) AS TOn, TimeOff AS TOff,
    StationNr, Alarmcode, ID, Parameter
    FROM tblAlarmLog WHERE TimeOff IS NULL
    """
    file_name = f'../DATA/SUM/{period}-sum.mdb'
    alarms = mdb_to_df(file_name=file_name, sql=usecols_sum)

    alarms['TOn'] = sqldate_to_datetime(alarms['TOn'])
    alarms['TOff'] = sqldate_to_datetime(alarms['TOff'])

    alarms.rename(columns={'TOn': 'TimeOn',
                           'TOff': 'TimeOff'}, inplace=True)

    alarms = alarms[alarms.StationNr >= 2307405]

    alarms = alarms[
        alarms.StationNr <= 2307535].reset_index(
        drop=True)

    alarms.dropna(subset=['Alarmcode'], inplace=True)

    alarms.reset_index(drop=True, inplace=True)

    alarms.Alarmcode = alarms.Alarmcode.astype(int)

    return alarms


def sqldate_to_datetime(column):
    try:
        column = column.str.replace(',', '.').astype(float)
    except:
        pass
    day_parts = np.modf(column.loc[~column.isna()])
    column = column.fillna(0)

    column.loc[~column.isna()] = (
        dt(1899, 12, 30) +
        day_parts[1].astype('timedelta64[D]', errors='ignore') +
        (day_parts[0] * 86400000).astype('timedelta64[ms]', errors='ignore')
    )
    return column

# %% [markdown]
# # Calcul mois


# %%
period = '2020-07'

alarms = read_sum(period)

results = pd.read_pickle(f'../DATA/results/{period}.pkl')

alarms.rename(columns={'StationNr': 'StationId'}, inplace=True)

alarms['StationId'] = alarms['StationId'] - 2307404
results['StationId'] = results['StationId'] - 2307404


# %%
results


# %%
Frame = pd.DataFrame
c1 = 'Alarmcode'
c2 = 'Error Group'

error_group = pd.concat([Frame({c1: range(901, 2101), c2: 'System'}),
                         Frame({c1: range(2101, 3000), c2: 'Generator'}),
                         Frame({c1: range(3100, 4000), c2: 'Hub'}),
                         Frame({c1: range(4100, 5000), c2: 'Gear'}),
                         Frame({c1: range(5000, 6000), c2: 'Grid'}),
                         Frame({c1: range(6100, 7000), c2: 'Rotor'}),
                         Frame({c1: range(7100, 8000), c2: 'Hydraulics'}),
                         Frame({c1: range(8000, 8400), c2: 'Environement'}),
                         Frame({c1: range(8450, 9000),
                                c2: 'Turbine cond...'}),
                         Frame({c1: range(9100, 10000), c2: 'Brake'}),
                         Frame({c1: range(10100, 11000), c2: 'Yaw'}),
                         Frame({c1: range(11100, 12000), c2: 'PFC'}),
                         Frame({c1: range(12100, 13000), c2: 'Transformer'}),
                         Frame({c1: range(13000, 14000), c2: 'Converter-1'}),
                         Frame({c1: range(14000, 15000), c2: 'Gen.inverter'}),
                         Frame({c1: range(15000, 16000), c2: 'Grid inverter'}),
                         Frame({c1: range(16000, 17000), c2: 'Main bearing'}),
                         Frame({c1: range(17000, 18300), c2: 'Converter-2'}),
                         Frame({c1: range(62001, 64000), c2: 'Controller'}),
                         Frame({c1: range(64000, 65200), c2: 'MISCELLANEOUS'})]
                        )


# %%
reorder = ['System',
           'Generator',
           'Hub',
           'Gear',
           'Grid',
           'Rotor',
           'Hydraulics',
           'Environement',
           'Turbine cond...',
           'Brake',
           'Yaw',
           'PFC',
           'Transformer',
           'Converter-1',
           'Gen.inverter',
           'Grid inverter',
           'Main bearing',
           'Converter-2',
           'Controller',
           'MISCELLANEOUS']


# %%
error_list = pd.read_excel(
    r'Error_Type_List_Las_Update_151209.xlsx',
    usecols=lambda x: x != 'Type Selected')

error_list.Alarmcode = error_list.Alarmcode.astype(int)  # ,errors='ignore'

error_list.drop_duplicates(subset=['Alarmcode'], inplace=True)

error_list = error_list.merge(error_group, on='Alarmcode', how='left')

# ------------------------------------------------------------------------
''' label scada alarms with coresponding error type
and only keep alarm codes in error list'''
result_sum = pd.merge(alarms, error_list[[
    'Alarmcode', 'Error Type', 'Error Group']],
    on='Alarmcode',
    how='inner', sort=False)

# Remove warnings
result_sum = result_sum.query('`Error Type` in [1, 0]')

# apply cascade
alarms_result_sum = apply_cascade(result_sum)

# only keep  parent alarms
parent_result_sum = alarms_result_sum.query('TimeOn == NewTimeOn')

# dash duree
main_result_sum = alarms_result_sum.query('RealPeriod > @pd.Timedelta(0)')


# %%
''' label scada alarms with coresponding error type
and only keep alarm codes in error list'''
result_sum = pd.merge(alarms, error_list[[
    'Alarmcode', 'Error Type', 'Error Group']],
    on='Alarmcode',
    how='inner', sort=False)

# Remove warnings
result_sum = result_sum.loc[result_sum['Error Type'].isin([1, 0])]

# apply cascade
alarms_result_sum = apply_cascade(result_sum)

# only keep  parent alarms
parent_result_sum = alarms_result_sum.query('TimeOn == NewTimeOn')

# dash duree
main_result_sum = alarms_result_sum.query('RealPeriod > @pd.Timedelta(0)')

# %% [markdown]
# ## Graphe 3 ax5

# %%
df_ax5 = (main_result_sum.groupby('Error Group')
          .agg(Freq=('Alarmcode', 'count'),
               Duree=('RealPeriod', lambda x: x.sum().total_seconds()/3600))
          .reindex(reorder)
          .dropna()
          .reset_index()
          )

# df_ax5.plot(kind='bar', x='Error Group', ax=ax5)

# %% [markdown]
# ## Graphe 4 ax6

# %%
df_ax6 = (main_result_sum.groupby('Alarmcode')
          .agg(Freq=('Alarmcode', 'count'),
               Duration=('RealPeriod',
                         lambda x: x.sum().total_seconds()/3600))
          .sort_values('Duration', ascending=False)
          .head(20)
          .reset_index()
          .sort_values('Duration', ascending=False))

# df_ax6.plot(kind='bar', x='Alarmcode', ax=ax6)

# %% [markdown]
# ## Graph 6 ax9

# %%
df_ax9 = pd.merge((results.groupby('StationId')
                   .agg(Duration_115=('Duration 115(s)',
                                      lambda x: x.sum()/3600),
                        Duration_20_25=('Duration 20-25(s)',
                                        lambda x: x.sum()/3600))
                   .sort_values('Duration_115', ascending=False)
                   # .head(25)
                   .reset_index()
                   ),
                  (alarms.groupby('StationId')
                   .agg(Freq_115=('Alarmcode',
                                  lambda x: int(x[x == 115].count()/2)),
                        Freq_25=('Alarmcode',
                                 lambda x: x[x == 20].count()))
                   .reset_index()
                   ),
                  on='StationId'
                  ).sort_values('Duration_115', ascending=False).head(20)


# df_ax9.plot(kind='bar', x='StationId', ax=ax9)

# %% [markdown]
# ## table 1 ax1

# %%
df_ax1 = pd.DataFrame(columns=['LTA-Lost Time', 'Indispo. Tarec',
                               'Indispo. ONEE', 'Indispo. Ebop',
                               'Indispo. Siemens',
                               'Pertes éléctriques \n en MWh',
                               'Power Boost en MWh',
                               'Performance moyenne \n des turbines',
                               'MTBF - Mean Time \n Between Failure',
                               'MTTR - Mean Time \n To Repair',
                               'MTTI - Mean Time \n To Intervention'],
                      index=['Indicateurs \n annuels :',
                             f'Indicateurs du \n mois {period} :'])


# %%
df_ax1

# %% [markdown]
# # Calcul Cumul

# %%
cumul_alarms = pd.DataFrame()
cumul_results = pd.DataFrame()

month = 1
for month in range(1, 8):

    month = str(month)

    alarms = read_sum(f'2020-{month.zfill(2)}')
    cumul_alarms = pd.concat([cumul_alarms, alarms])
    # -------------------------------------------------------------------------
    results = pd.read_pickle(f'../DATA/results/2020-{month.zfill(2)}.pkl')
    results = results[['StationId', 'ELNX',
                       'Duration 115(s)', 'Duration 20-25(s)',
                       'Period 0(s)', 'Period 1(s)',
                       'EL_indefini_left']]
    cumul_results = pd.concat([cumul_results, results])


# %%
pd.read_pickle(f'../DATA/results/2020-{month.zfill(2)}.pkl').columns


# %%
cumul_alarms.rename(columns={'StationNr': 'StationId'}, inplace=True)

cumul_alarms['StationId'] = cumul_alarms['StationId'] - 2307404
cumul_results['StationId'] = cumul_results['StationId'] - 2307404


# %%
cumul_result_sum = pd.merge(cumul_alarms, error_list[[
    'Alarmcode', 'Error Type', 'Error Group']],
    on='Alarmcode',
    how='inner', sort=False)

# Remove warnings
cumul_result_sum = cumul_result_sum.query('`Error Type` in [1, 0]')

# apply cascade
cumul_alarms_result_sum = apply_cascade(cumul_result_sum)

# only keep  parent alarms
cumul_parent_result_sum = cumul_alarms_result_sum.query('TimeOn	 == NewTimeOn')

cumul_main_result_sum = cumul_alarms_result_sum.query(
    'RealPeriod > @pd.Timedelta(0)')

# %% [markdown]
# ## Graphe 1 ax3

# %%
df_ax3 = (cumul_main_result_sum.groupby('Error Group')
          .agg(Freq=('Alarmcode', 'count'),
               Duree=('RealPeriod', lambda x: x.sum().total_seconds()/3600))
          .sort_values('Freq', ascending=False)
          .reindex(reorder)
          .dropna()
          .reset_index()
          )

# df_ax3.plot(kind='bar', x='Error Group', ax=ax3)

# %% [markdown]
# ## Graphe 5 ax8

# %%
df_ax8 = pd.merge((cumul_results.groupby('StationId')
                   .agg(Duration_115=('Duration 115(s)',
                                      lambda x: x.sum()/3600),
                        Duration_20_25=('Duration 20-25(s)',
                                        lambda x: x.sum()/3600))
                   .sort_values('Duration_115', ascending=False)
                   # .head(25)
                   .reset_index()
                   ),
                  (cumul_alarms.groupby('StationId')
                      .agg(Freq_115=('Alarmcode',
                                     lambda x: int(x[x == 115].count()/2)),
                           Freq_25=('Alarmcode',
                                    lambda x: x[x == 20].count()))
                      .reset_index()
                   ),
                  ).sort_values('Duration_115', ascending=False).head(20)

# df_ax8.plot(kind='bar', x='StationId', ax=ax8)

# %% [markdown]
# ## Graphe 7 ax18

# %%
cumul_results.columns


# %%
df_ax18 = (cumul_results[['StationId', 'ELNX', 'EL_indefini_left']]
           .groupby('StationId')
           .sum()
           .sort_values('ELNX', ascending=False)
           # .head(20)
           .reset_index())


# %% [markdown]
# # Export Data to excel


# %%
writer = pd.ExcelWriter('output.xlsx')

workbook = writer.book
dashsheet = workbook.add_worksheet('Dash')

df_ax3.to_excel(writer, index=False, sheet_name='ax3')
worksheet = writer.sheets['ax3']


def make_chart_ax3():
    column_chart = workbook.add_chart({'type': 'column'})

    column_chart.add_series({'values': ['ax3', 1, 1, 13, 1],
                             'categories': ['ax3', 1, 0, 13, 0],
                             'name': ['ax3', 0, 1]})

    line_chart = workbook.add_chart({'type': 'line'})

    # Configure the data series for the secondary chart. We also set a
    # secondary Y axis via (y2_axis).
    line_chart.add_series({
        'name': ['ax3', 0, 2],
        'categories': ['ax3', 1, 0, 13, 0],
        'values': ['ax3', 1, 2, 13, 2],
        'y2_axis': True,
    })

    # Combine the charts.
    column_chart.combine(line_chart)

    # Configure the chart axes.
    # column_chart.set_x_axis({'name': df_ax3.columns[0]})
    column_chart.set_y_axis({'name': df_ax3.columns[1]})
    column_chart.set_legend({'position': 'bottom'})
    column_chart.set_title({'name': 'Cumul annuel par type d\'alarme',
                            'name_font': {'size': 12, 'bold': True}})
    line_chart.set_y2_axis({'name': df_ax3.columns[2]})
    return column_chart


column_chart = make_chart_ax3()
# Insert the chart into the worksheet.
worksheet.insert_chart('E2', column_chart)

column_chart = make_chart_ax3()
# Insert the chart into the worksheet.
dashsheet.insert_chart('E42', column_chart)


# -------------------------------------------------------------------------------------------
df_ax5.to_excel(writer, index=False, sheet_name='ax5')

worksheet = writer.sheets['ax5']


def make_chart_ax5():
    column_chart = workbook.add_chart({'type': 'column'})

    column_chart.add_series({'values': ['ax5', 1, 1, 13, 1],
                             'categories': ['ax5', 1, 0, 13, 0],
                             'name': ['ax5', 0, 1]})

    line_chart = workbook.add_chart({'type': 'line'})

    # Configure the data series for the secondary chart. We also set a
    # secondary Y axis via (y2_axis).
    line_chart.add_series({
        'name': ['ax5', 0, 2],
        'categories': ['ax5', 1, 0, 13, 0],
        'values': ['ax5', 1, 2, 13, 2],
        'y2_axis': True,
    })

    # Combine the charts.
    column_chart.combine(line_chart)

    # Configure the chart axes.
    # column_chart.set_x_axis({'name': df_ax5.columns[0]})
    column_chart.set_y_axis({'name': df_ax5.columns[1]})
    column_chart.set_legend({'position': 'bottom'})
    column_chart.set_title({'name': f'Type d\'alarme {period}',
                            'name_font': {'size': 12, 'bold': True}})

    line_chart.set_y2_axis({'name': df_ax5.columns[2]})
    return column_chart


# Insert the chart into the worksheet.
column_chart = make_chart_ax5()
# Insert the chart into the worksheet.
worksheet.insert_chart('E2', column_chart)

column_chart = make_chart_ax5()
# Insert the chart into the worksheet.
dashsheet.insert_chart('U42', column_chart)


# --------
df_ax6.to_excel(writer, index=False, sheet_name='ax6')

worksheet = writer.sheets['ax6']


def make_chart_ax6():
    column_chart = workbook.add_chart({'type': 'column'})

    column_chart.add_series({'values': ['ax6', 1, 1, 20, 1],
                             'categories': ['ax6', 1, 0, 20, 0],
                             'name': ['ax6', 0, 1]})

    line_chart = workbook.add_chart({'type': 'line'})

    # Configure the data series for the secondary chart. We also set a
    # secondary Y axis via (y2_axis).
    line_chart.add_series({
        'name': ['ax6', 0, 2],
        'categories': ['ax6', 1, 0, 20, 0],
        'values': ['ax6', 1, 2, 20, 2],
        'y2_axis': True,
    })

    # Combine the charts.
    column_chart.combine(line_chart)

    # Configure the chart axes.
    column_chart.set_x_axis({'name': df_ax6.columns[0]})
    column_chart.set_y_axis({'name': df_ax6.columns[1]})
    column_chart.set_legend({'position': 'bottom'})
    column_chart.set_title({'name': f'Alarmes {period}',
                            'name_font': {'size': 12, 'bold': True}})

    line_chart.set_y2_axis({'name': df_ax6.columns[2]})
    return column_chart


# Insert the chart into the worksheet.
column_chart = make_chart_ax6()
# Insert the chart into the worksheet.
worksheet.insert_chart('E2', column_chart)

column_chart = make_chart_ax6()
# Insert the chart into the worksheet.
dashsheet.insert_chart('AC42', column_chart)


# ------------------------------------------------------------------------------------------------
df_ax8.to_excel(writer, index=False, sheet_name='ax8')

worksheet = writer.sheets['ax8']


def make_chart_ax8():
    column_chart = workbook.add_chart({'type': 'column'})

    column_chart.add_series({'values': ['ax8', 1, 3, 20, 3],
                             'categories': ['ax8', 1, 0, 20, 0],
                             'name': ['ax8', 0, 3]})

    column_chart.add_series({'values': ['ax8', 1, 4, 20, 4],
                             'categories': ['ax8', 1, 0, 20, 0],
                             'name': ['ax8', 0, 4]})

    line_chart = workbook.add_chart({'type': 'line'})

    # Configure the data series for the secondary chart. We also set a
    # secondary Y axis via (y2_axis).
    line_chart.add_series({
        'name': ['ax8', 0, 1],
        'categories': ['ax8', 1, 0, 20, 0],
        'values': ['ax8', 1, 1, 20, 1],
        'y2_axis': True,
    })

    line_chart.add_series({
        'name': ['ax8', 0, 2],
        'categories': ['ax8', 1, 0, 20, 0],
        'values': ['ax8', 1, 2, 20, 2],
        'y2_axis': True,
    })

    # Combine the charts.
    column_chart.combine(line_chart)

    # Configure the chart axes.
    column_chart.set_x_axis({'name': df_ax8.columns[0]})
    column_chart.set_y_axis({'name': 'Freq'})
    column_chart.set_legend({'position': 'bottom'})
    column_chart.set_title({'name': 'Arrêts turbines : Cumul Annuel',
                            'name_font': {'size': 12, 'bold': True}})

    line_chart.set_y2_axis({'name': 'Duration'})
    return column_chart


# Insert the chart into the worksheet.
column_chart = make_chart_ax8()
# Insert the chart into the worksheet.
worksheet.insert_chart('G2', column_chart)

column_chart = make_chart_ax8()
# Insert the chart into the worksheet.
dashsheet.insert_chart('E58', column_chart)


# ----------------------------------------------------------------------------------------
# --------
df_ax9.to_excel(writer, index=False, sheet_name='ax9')

worksheet = writer.sheets['ax9']


def make_chart_ax9():
    column_chart = workbook.add_chart({'type': 'column'})

    column_chart.add_series({'values': ['ax9', 1, 3, 20, 3],
                             'categories': ['ax9', 1, 0, 20, 0],
                             'name': ['ax9', 0, 3]})

    column_chart.add_series({'values': ['ax9', 1, 4, 20, 4],
                             'categories': ['ax9', 1, 0, 20, 0],
                             'name': ['ax9', 0, 4]})

    line_chart = workbook.add_chart({'type': 'line'})

    # Configure the data series for the secondary chart. We also set a
    # secondary Y axis via (y2_axis).
    line_chart.add_series({
        'name': ['ax9', 0, 1],
        'categories': ['ax9', 1, 0, 20, 0],
        'values': ['ax9', 1, 1, 20, 1],
        'y2_axis': True,
    })

    line_chart.add_series({
        'name': ['ax9', 0, 2],
        'categories': ['ax9', 1, 0, 20, 0],
        'values': ['ax9', 1, 2, 20, 2],
        'y2_axis': True,
    })

    # Combine the charts.
    column_chart.combine(line_chart)

    # Configure the chart axes.
    column_chart.set_x_axis({'name': df_ax9.columns[0]})
    column_chart.set_y_axis({'name': 'Freq'})
    column_chart.set_legend({'position': 'bottom'})
    column_chart.set_title({'name': f'Arrêts turbines {period}',
                            'name_font': {'size': 12, 'bold': True}})

    line_chart.set_y2_axis({'name': 'Duration'})
    return column_chart


# Insert the chart into the worksheet.
column_chart = make_chart_ax9()
# Insert the chart into the worksheet.
worksheet.insert_chart('G2', column_chart)

column_chart = make_chart_ax9()
# Insert the chart into the worksheet.
dashsheet.insert_chart('M58', column_chart)

# ----------------------------------------------------------------------------------------
df_ax18.to_excel(writer, index=False, sheet_name='ax18')

worksheet = writer.sheets['ax18']


def make_chart_ax18():
    column_chart = workbook.add_chart({'type': 'column',
                                       'subtype': 'stacked'})

    for col in range(1, 3):
        column_chart.add_series({'values': ['ax18', 1, col, 20, col],
                                 'categories': ['ax18', 1, 0, 20, 0],
                                 'name': ['ax18', 0, col]})

    # Configure the chart axes.
    column_chart.set_x_axis({'name': df_ax18.columns[0]})
    # column_chart.set_y_axis({'name': df_ax18.columns[1]})
    column_chart.set_legend({'position': 'bottom'})
    column_chart.set_title(
        {'name': 'Energie perdue selon FSA cumulée sur l\'année 2020 en MWh',
         'name_font': {'size': 12, 'bold': True}}
    )
    return column_chart


# Insert the chart into the worksheet.
column_chart = make_chart_ax18()
# Insert the chart into the worksheet.
worksheet.insert_chart('E2', column_chart)

column_chart = make_chart_ax18()
# Insert the chart into the worksheet.
dashsheet.insert_chart('U58', column_chart)


writer.save()
# %%

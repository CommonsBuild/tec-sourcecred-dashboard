from bokeh.layouts import gridplot
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.palettes import RdBu3
from bokeh.plotting import figure
from datetime import datetime, timedelta
import holoviews as hv
import hvplot.pandas
import numpy as np
import pandas as pd
import panel as pn
import param
import time

from cred.cred_data import CredData
from cred.utils.io import fetch_cred_data
from cred.utils.plot import pie_chart

pn.extension()

tec_logo = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAASIAAABPCAYAAABPo8iGAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAA44SURBVHgB7Z1dltM4Fsdv+vA+xQrGsIDhY96HFAsYoDdAqhfAR887lWIBfPT7VIUNDAULoEIvAIpZAOXuBTTpDaDWP5YSRZZs2ZYTu3x/5+gksWVJlq1/rmTpmohhGIZhGIZhGIbZMSNqGSHETfnxmKpzNBqNUiMdpHGT4rGQ6T8NjSzz35MfZ5HLEItrZl3FQJ7vRH6cUHtcyHA3drmZftKqECkRQuNFI57J8DHw0HuUNfh9GRYyvJRhYuw/leEd1Qdp35ON4FpIZEuE5vK4feoIsmxo0PsxG7RM8yFl1+u1DOfUDmMZ/iXLfZ1qIsuJD1yTvZKo5zKfhYq/R+4/E/wxnVtpJyqk1p8iqfIv06bsHvWlu0KmMQ9I17mPqQlESIZvYs2k4vEzNDIVXDykmshjx6oBh8Tdk+Gzke8ZdQhVPwlFAvWqznNKLYEGJ8OBDN+pAfL4Ea6HKOY76kfledW6lna8h1baM7suVDpTtf2rkfa+KOar5/g9T9pTGhBXqAXEpiVUC/lvMMGNIL/6BAc3CeK9oZYQ3e6ORUesLSF0i6fUfWBCwDJO1e+EMksFv+dGHFgssP4/UHYt8fvUSEcfdyLr4DdYLs7MMosFVvRjle5y+EBtN5k5Dv/m2Ia0nsnwMzFxEXlLSDOhGhj/Sj4qW0Yi0CKScd468ruUFpHYgiVk5BXFIvKkCT449o/UPlg+Y8e+z2rfxNi2YRFZ275aeWuL6HtAOafG/bQqjxiwRfQDRUREsIRsYBnJjyKrZyYadNN8yDQxUHufBoDonyXUlNSxbVF0gFiP36Cu8OMuxSkHLLVjYXTRhki0rlkbIqRR3TR83Uo3TYnQhAaAum6vaDgiVIc7Mkxpff/NCgaSRx5r5tQcDFfMKWsv+MN7ovIYJFGEqE0RMsCFukH+8ZooYjRAEcJ1e80iVMiY1k/J9LhUEYfWbxyTUv4JJLb/pNLGWNEpDZTGXbMtiRAEBqYzHpsXPU5u1E1jEWI8QCAOKLv30JUqmhcHcdm3wl3yiwwGsZ+rdNuct9VpGllE2xIhjZoLsk/FT7JqWUYsQkwBX2g9D+6TDGNZh09k3b1yRfY9dfPExfVAOv+mzDJKaIDUtoi2LUKaNiwjFqHBcUd/EZuTCL1AMCibDa6tl2ci3vwt3UXTEyMHRy0h2pUIaWKKUQ0R6u2NwiK0fPoKEVlNlqVMXGDlJLQ5/yiHEqNXKs5VGY5d0cR6Iq4ZPpSkq0VukFTumjUQob2K/yALJThOKnTT8ETCTmdZ9pqWEOZJCeoZMURInfaE4nQf8OBhQVtCnjMu24H6iT+oxIoSuvYNlYAJiLqLhvvLHv9JPMcVlc/uojE+hH+yYhtMAstkL8EYIkngdZtSTUR+Il5T/hCZODJMeNdM7Lg75kNZTU0WwF5qzOvW0BKaUv6xdF3wpOiuY14NM1CChKirIsQUE+O6sQgx26BUiFiE+gmLENMnCoWIRaifsAgxfcMrRCxC/STidZtSPBFaLhJlEWJ8OIWIRaifRL5uwRNCS1hO1mMRYorIzSNiESrlQDaqGW0RkfmrOSuJ08XrpkVoRi0hNt22+rDdubrcwup9G0s0RLF72XM9182Xhth0Z3tuzo1z7RNh7m9TwyHbmKqdu5OCc17VlZmfvYzFt6+k/tzucMV25wmVMaFARNz5LdHKFQuROXIr4r4IuG4V87wQzVg5GWsTsXZ4VlSORFjOy4Qxh8naZzs8u1ZQF5+E8iMkPM7RVPn08S8cZb8Qm87Rytzf2s7bqp67L96hdd4bTubEui50/PuOerLPvaz+lq52EfeKcRBbQv3lhAZmCXk4pfxsbeHYhjUVEAWvczOxdguLhphQfvkHGuIttf82hfFEpvu+wqLYc3IvY0od20LPfUGbM8Fx3+BcDmXZYJWFzMnTztzmvtUPqv6wDMZXf2NS9Svj3r6iDkLEt8Qi1FdYhLJ8X5f4mzaBleldQa9IVMATv33djVBpvZfhf1TNn7luwLeLli8ZvAuchIoCPfUtT7HO/ZuMd2DsQ5mwXAVLbq5SOIhb5m8b92WiyvfA6uJBhL6q/Xs/KBE6o4G6H2CislyHtQMRqgpE4E8KX0G/cDRytBk06J8onJSyBviEukWIKNqklFl448D4qzyMRb66/hawiFiEmFg8L7Ew2gR390NHw0B349TahkbxC2XdM6ygD/I/LfIuQ1IKRy+WRX4QQJTpS8kxd0R+feDCUcc4d4iCLSiucydLfHFsQtXQbkvQ7cL5BHVNRX7gOtX7rhCLEBOHow64Fpk4tmH8zG6MaBHmSndYKGWiYOZxaKUVumZTe2PUAvjPkvhjyj8VgyXhEnuX10jXuV9Tadh8pmquaueUvYAT+aI+fgk87gFtuk9B/V1v5b1mzODogggtx0koLyhpQXz8q2N8BGMd/6EwMM4xo/Ugb1VMATwsifuG8u9Ic3Wj9Lmk1vaU3MyN7wnVM0aQ5xFlb2WGGP0/8DiI4Ex9n+iNLERMU7ogQpovoU+klP8f7YzshQqlx0hOVbcKlkUdITIFEEJUND6TVnjCNh+FvaL6wnxlOh6v0/rFkxNyW1s5VP3BwtNdtND6myOoQfKJ3sdCVJ2qDt5ikFA36ZIIVaaCM7LlNbca+j2qn6cpgLCsKs3xikxKmS/uh1Tx6as6lzmtu2hF3FF5mRM5V7AQVeelCkOnayKEf9i3jgHb5To3Cuui2Q3xXB2XyPDZSjtRn5Xdu1YQwMci7+5YP6o35/vg3M8cUxTKzj0GZhct0RvVOaaUdS9xDjNr4F3XNQbUUxYipg5dtYT2KC8mIS5aTQvF3L70GEDrSXl22k3qwRTAv3ni+M7HNd8n8eTRKo4umrkPs6eL3PNigHz5xHIZk7rJQeh8FKW0ZQN/g2ekOukhyDrFTeKarNfr7hjTTRq/YJG5tPxIeZOeRYhpBRYixsfyrRa0FqOXLEJMW7AQMU6MafhajO7v4GkhMxBYiBgvlhjhxwcWI6YNWIgGhKjwGm6NJUZ4WnMh4nBMDKPA4/sZVQOzSYfsLiSlgtcS12RbdYq5HBCXN1UOMh5xw//OmOIANxwvZNo/EzN4rpi+SUJQq5sHLURV66yMLddpbTGiTIRn1BA1YwQJ/peK/dlUSc9c1W2Sc0cqCtyxhrhMdaSVqGC6cN3Y5jmuyIXsmBw4ype48rH2uVzjus7f7bq1i4jmLkRDmVQo01RsjzOKjNhenZpU7qZFPF/tivQ7NURUcEdqxC+6X46tuNrFKl6RvefI33QFO3Xk8bWgzGDlJtZI78RTNpdL11U+ZvmsfceB579RV9uEZ1YPl1qWUZcQFdyRynBdxccKfd2YsXhVWyJ6NT0EcuHoMiKfJ5S9ZsnMf0LFawETCI21eHVEjgm4IlsI+oLWi0Fnxm7TpeufDp9EOM9Cj4mqvAdG3jNj95iMuqItw0I0bE76Lka06Y5033LnijGtT7QWCjT0G+r7iYy78q4oNl8oedOT1yMZ75XRjUJ6z6gYHWdu5JNQtgjUhc57Y/KoEqkTypZK/MNzbIhP7L+rz1M9xCDyrlu3Dj81GzbLm1vssJsWE8f4BgbYfe5cf3ds+42K0VaROY6TUDljsek5suw4JJ4GbDPBPu0TO2S88Zv+YjwZhRht3RoCbBExWoz6bhltoBoXLJcZxUFbQY8os5xMa2hO/qeJKWWio60ifRy2QzB8C16rMqfMD/djKu6iabFFFxTCanoUwKD5a9oBLEQMuJRiFBk0WNTNM2VB/kqZ+Mwoqz8fc8qESFtF19TvI8q6WbGEyHTHAavtvR1BTcNA9w7dM3RBbcduGLDek/GOaMtw14zRXKpuWgssX1dEmdUxoawh68ZfBlyMLJ37U2atoBvUhuBrdxx6PMk1PQEfUxn2rfCjOu5wF0/O2CJiTC6NZSTWc4CWYzqRFuyioUOM9FMnNPaycSUUZK7CRG2DeKUUGYfHxI0/FbGeP4SQWnOSRrRD2CJibHprGQnjNdKKCWWi4XqyZT95GpH/aZQJGjm6aWjVod4ZzbgQs1lJ/JGnfDcoLC8tdK5Ba3THIKCPVgesn+TtDLaIqoO+/gXFJaFugZvedu0Zm1gzySEKmDUMEYI719TYl6hPbd2hxX2kzFJ4IDLPgi73r7+6MjK8EeKVOHtqFnWpJaGOO1PHUcFxKB/GdsYyPJVxHlj7EyMOlZQx5zFRod9y8sC6j/X1SKneCxcb0WUhOlEDa10koWGQULs08g7qcOcKMUqsaDM9X8YYrAWYbexyxXpU1I0z3kRRuaxU8t4wVT74Q8cANiy5xIqydC/remmiI525yu++tR2vlodIHTvSXy5uHoW9DjsqlfuFSkUT6hap+kyI6Qu6Uc2IGTyXYYwopWzUPyWmL7AIMRv0XYhSMqb1M72ARYjJ0WchSolFqG+wCDFO+ipEKbEI9Q0WIcZLH4UoJRahPvKcRYjx0TchSolFqI8cjfhVREwBfRKilFiE+giLEFNKX4QIM2dvsQj1DhYhJog+CBFEaH8Xsz2ZRrAIMcF0XYhYhPoJixBTiS4LEYtQP2ERYirTVSFiEeonLEJMLbooRCxC/YRFiKlN14RoyCJ0rkIfYRFiGtElIRq6CGnfwX0TIxYhpjFdEaLBixDOXZ1/n8SIRYiJQheEiEVo89zx/S51X4xYhJho7FqIWISsc1cuRbX7066KEYsQE5VdChGLkOfcOy5GLEJMdHYlRG/kzXyLRchPR8WIRYhphV0IEURoQsMkpQpWYMfEiEWIaY1tCxGLUEUrsCNixCLEtMo2hYhFqKYbkx2LEYsQ0zp1XrCYUnU+bkGEvlA3gQX0dNTQl5L1MkH9csC2eSfzfUUM0zJ/ATdZKPwKK8UlAAAAAElFTkSuQmCC"

react = pn.template.ReactTemplate(title='TECred Dashboard',
                                  header_background= '#7622a8',
                                  logo=tec_logo)
# pn.config.sizing_mode = 'stretch_both'
pn.config.sizing_mode = 'stretch_width'

# TODO: Get from URI params
# BASE_URI = 'https://raw.githubusercontent.com/1Hive/pollen/gh-pages'
BASE_URI = 'https://raw.githubusercontent.com/TECommons/tec-sourcecred/gh-pages'

# TODO: replace by periods (All-time, Last week, Last month, Last year)
DEFAULT_DATERANGE = (datetime.fromtimestamp(int(time.time())) - timedelta(days=30),
                     datetime.fromtimestamp(int(time.time()))
                    )

# STEP 1: Loading data
cred_data = fetch_cred_data(uri=f'{BASE_URI}/output/credResult.json', filename='data/credResult.json', cache=False)
accounts_data = fetch_cred_data(uri=f'{BASE_URI}/output/accounts.json', filename='data/accounts.json', cache=False)

cred = CredData(cred_data, accounts_data)

# STEP 2: Creating dashboard
user_filter = pn.widgets.AutocompleteInput(options=cred.get_user_nodes().user.unique().tolist(),
                                           placeholder='Browser by username',
                                           restrict=False, case_sensitive=False
                                          )

class CredDashboard(param.Parameterized):
    
    # TODO: use intervals
    date_range = param.DateRange(default=DEFAULT_DATERANGE, bounds=DEFAULT_DATERANGE, label='Date Interval')
    top_n = param.Integer(default=5, bounds=(5, 100), step=5, label="Compare with Top-")
    user = param.String(label='Find a user', default='')
    
    def __init__(self, cred: CredData, **params):
        super(CredDashboard, self).__init__(**params, name="Filters")
        snapshot_int = cred.get_dt_intervals()
        self.param.date_range.default = (snapshot_int[0], snapshot_int[-1]) 
        self.param.date_range.bounds = (snapshot_int[0], snapshot_int[-1])
        self.cred = cred
        self.intervals = cred.get_dt_intervals()
        self.df = cred.to_df()

        self.selectedUser = None
    
    def set_user(self, value):
        print(f"******====== SET_USER ******====== {value}")
        if value is not None:
            self.selectedUser = value.new
            self.user = value.new
            user_filter.disabled = True
        else:
            self.selectedUser = None
            self.user = ''
            user_filter.value = ''
            user_filter.disabled = False
    
    def view_distr_stats(self):
        x = [4., 10.]
        y = [1.5, 1.5]
        source = ColumnDataSource(dict(
            x=x,
            y=y,
            color=[RdBu3[2], # red
                   RdBu3[0] # blue
                  ],
        ))

        p = figure(plot_height=300,tools=[], x_range=(0, 14), y_range=(0, 3))

        p.circle(x='x', y='y', radius=2., color='color', source=source)
        p.text(np.array(x), np.array(y), text=['{}\nCRED'.format(round(self.cred.distributed_cred, 2)),
                                               '{}g\nGRAIN'.format(round(self.cred.distributed_grain, 2))
                                              ],
               text_baseline="middle", text_align="center", text_color='#000000', text_font_size={'value': '25px'})

        p.grid.visible = False
        p.axis.visible = False
        p.outline_line_color = None

        grid = gridplot([[p]], merge_tools=True, sizing_mode='scale_height', 
                        toolbar_options=dict(logo=None))

        return pn.Row(grid)
    
    def view_cred_grain_over_time(self):
        df_grain_distr = self.cred.get_grain_distribution()
        df_grain_overall = df_grain_distr.groupby('credTimestampMs').sum()
        df_cred_overall = cred.get_cred_over_time()
        df_cred_overall.hvplot.line(label='Cred')
        
        
        custom_hover = HoverTool(tooltips=[("Date",  "@credTimestampMs{%Y/%m/%d}"),
                                           ("Amount","@amount{0.00}"),
                                          ],
                                 formatters = {
                                     '@credTimestampMs': 'datetime'
                                 }
                                )
        return (df_cred_overall.hvplot.line(label='Cred').opts(tools=[custom_hover]) * 
                df_grain_overall.hvplot.line(label='Grain').opts(tools=[custom_hover])).opts(legend_position='top_left',
                                                                                             title='Distribution Over Time',
                                                                                             xlabel='Date',
                                                                                            )
    
    def view_ranking(self):
        df_rank = self.cred.get_user_ranking()
        line_alpha = [0.2] * self.top_n
        df_ucred = None
        if self.user:
            user_rank = df_rank.query(f'user=="{self.user}"').index[0] + 1
            if user_rank > self.top_n:
                df_ucred = pd.DataFrame([self.intervals, df_rank[df_rank.user == self.user].credOverTime.to_list()[0]]).T
                df_ucred.columns = ['date', f'{user_rank} - {self.user}']
                df_ucred.set_index('date', inplace=True)
                if user_rank > self.top_n:
                    line_alpha += [1.]
                else:
                    line_alpha[user_rank - 1] = 1.
        else:
            line_alpha = [1.] * self.top_n

        df_top = df_rank.head(self.top_n).copy()
        df_credtop = pd.DataFrame(df_top.credOverTime.to_list()).T
        df_credtop.columns = [f'{i + 1} - {u}' for i, u in enumerate(df_top.user.to_list())]
        df_credtop['date'] = self.intervals
        df_credtop.set_index('date', drop=True, inplace=True)
        df_plot = df_credtop.join(df_ucred, on='date') if self.user and user_rank > self.top_n else df_credtop

        return df_plot.hvplot.line(x='date',
                                   y=df_plot.columns.to_list(),
                                   xlabel='Date', ylabel='Cred', group_label='User Rank',
#                                    width=1000, height=400,
                                   line_alpha=line_alpha, hover_line_alpha=1.,
                                   hover_cols=['index'],
                                   title='Ranking + Cred over time'
                                  )
    
    def view_rank_ordered(self):
        df_rank = self.cred.get_user_ranking()
        df_topn = df_rank.head(self.top_n).copy()
        df_topn['rank'] = df_topn.index.map(lambda x: x + 1)#.map(str) + ' - ' + df_top100.user

        df_topn.sort_values('user', inplace=True)

        return df_topn.hvplot.bar(x='user', y='totalCred',
#                                   width=1000,
                                  rot=45,
                                  xlabel='User', ylabel='Total Cred', hover_cols=['rank'],
                                  title=f'Top-{self.top_n} Users sorted alfabetically'
                                 )
    
#     @param.depends('date_range', 'top_n')
    def view(self):
        return pn.Column(f'DateRange: {self.top_n} {self.date_range[0]} - {self.date_range[-1]}\n {self.user} / {self.selectedUser}')
    
#     @param.depends('top_n')
    def rank_table(self):
        cols = ['type', 'user', 'active', 'totalCred', 'credShare', 'grainBalance', 'grainPaid']
        return self.cred.get_user_ranking()[cols].head(self.top_n).hvplot.table(title= f'Top-{self.top_n} Users by Gained Cred')
    
    def view_cred_flow_analysis(self):
        df_nodes, df_edges = self.cred.get_cred_flow_from_graph()
        
        f_bar = hv.Bars(df_edges, 'index', ['forward', 'plugin']).opts(color='plugin',
                                                                       cmap='Category10',
                                                                       width=1000, height=300,
                                                                       xrotation=45,
                                                                       tools=['hover'],
#                                                                        xlabel='Edges',
                                                                       ylabel='Forward Cred Flow',
                                                                       xaxis=None
                                                                      )

        b_bar = hv.Bars(df_edges, 'index', ['backward', 'plugin']).opts(color='plugin',
                                                                        cmap='Category10',
                                                                        width=1000, height=500,
                                                                        xrotation=45,
                                                                        tools=['hover'],
                                                                        xlabel='Edges',
                                                                        ylabel='Backward Cred Flow'
                                                                       )
        
        n_bar = hv.Bars(df_nodes, 'index', ['weight', 'plugin']).opts(color='plugin',
                                                                      cmap='Category10',
                                                                      width=1000, height=500,
                                                                      xrotation=45,
                                                                      tools=['hover'],
                                                                      xlabel='Nodes',
                                                                      ylabel='Cred Flow'
                                                                     )
        
        df_n_byplugin = df_nodes.groupby('plugin').sum()
        df_e_byplugin = df_edges.groupby('plugin').sum()
        
        colors = ['#0F2EEE', '#0b0a15', '#DEFB48'][:df_e_byplugin.index.shape[0]]
        pie_radius = 0.4
        pieplot_height = 250
        pieplot_width = 400


        edge_pie_b = pie_chart(df_e_byplugin.backward,
                               index_name='plugin',
                               colors=colors,
                               title='Plugin Edges Backward',
                               radius=pie_radius,
                               plot_height=pieplot_height,
                               plot_width=pieplot_width,
                               toolbar_location=None,
                               show_legend=False
                            )

        edge_pie_f = pie_chart(df_e_byplugin.forward,
                               index_name='plugin',
                               colors=colors,
                               title='Plugin Edges Forward',
                               radius=pie_radius,
                               plot_height=pieplot_height,
                               plot_width=pieplot_width,
                               toolbar_location=None,
                               show_legend=False
                            )


        nodes_pie = pie_chart(df_n_byplugin.weight,
                              index_name='plugin',
                              colors=colors,
                              title='Plugin Nodes',
                              radius=pie_radius,
                              plot_height=pieplot_height,
                              plot_width=pieplot_width,
                            )

        return pn.Column(
#             "## Overall Cred Flow through Nodes/Edges",
            pn.Row(edge_pie_b, edge_pie_f, nodes_pie),
            (f_bar + b_bar).cols(1).opts(title='Cred flow through Edges', shared_axes=True),
            n_bar.opts(title='Cred flow through Nodes'),
        )



tecred_dashboard = CredDashboard(cred)

# Filter & Refesh
user_filter.param.watch(tecred_dashboard.set_user, ['options', 'value'], what='value', onlychanged=True)
refresh_button = pn.widgets.Button(name='\u27f3', width=30, sizing_mode='fixed')
refresh_button.on_click(lambda e: tecred_dashboard.set_user(None))

react.sidebar.append(pn.panel(tecred_dashboard, parameters=['top_n']))

react.main[:2,2:10] = tecred_dashboard.view_cred_grain_over_time
react.main[2:4, 4:8] = tecred_dashboard.view_distr_stats

main_view = pn.GridSpec(sizing_mode='stretch_both')
main_view[:2, :] = pn.Column(pn.Row(user_filter, refresh_button),
                             tecred_dashboard.view_ranking
                            )
main_view[3:5, :] = pn.Row(tecred_dashboard.rank_table, tecred_dashboard.view_rank_ordered)

react.main[4:,:] = pn.Tabs(("Cred Distribution", main_view),
                           ("How Cred is Distributed?", pn.Row(tecred_dashboard.view_cred_flow_analysis)
                           ),
                           tabs_location='above', active=0
                          )

react.servable();
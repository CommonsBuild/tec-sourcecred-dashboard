from datetime import datetime
import pandas as pd
from typing import Any, Dict, List, Tuple


class CredData():
    """
        Parses information from Sourcecred
        - Works with TimelineCred data format (sourcecred <= v0.7x)
    """
    
    def __init__(self, cred_data, accounts_data):
        self.cred_json_data = cred_data
        self.weighted_graph = cred_data[1]['weightedGraph'][1]
        self.cred_data = cred_data[1]['credData']
        self.accounts_data = accounts_data
        self.cache = {
            'df': None,
            'df_rank': None,
            'df_grain': None,
            'df_accounts': None,
            'df_cred_ot': None,
            'df_cred_eflow': None,
            'df_cred_nflow': None,
        }

    def get_weighted_graph(self, data) -> Dict[str, Any]:
        """
        Weighted graph from CredResult JSON data
        """
        return self.weighted_graph

    def get_cred_data(self) -> Dict[str, Any]:
        """
        Raw CredResult JSON data
        """
        return self.cred_data     
        
    def get_node(self, i: int) -> Dict[str, Any]:
        """
        Returns specifc node's information
        """
        node = dict()
        address = self.weighted_graph['graphJSON'][1]['sortedNodeAddresses'][i]
        node['address.source'] = f'{address[0]}/{address[1]}'
        node['address.nodeType'] = address[2]
        node['address.id'] = address[3]
        node['totalCred'] = self.cred_data['nodeSummaries'][i]['cred']
        node['credOverTime'] = self.cred_data['nodeOverTime'][i]['cred'] if self.cred_data['nodeOverTime'][i] else []
        node['description'] = self.weighted_graph['graphJSON'][1]['nodes'][i]['description']
        node['timestamp'] = self.weighted_graph['graphJSON'][1]['nodes'][i]['timestampMs']
        node['user'] = self.weighted_graph['graphJSON'][1]['nodes'][i]['description'] if node['address.nodeType'] == 'IDENTITY' else None
        
        return node
    
    @property
    def total_nodes(self) -> int:
        """
        Total amount of nodes (users, posts, etc) in the graph
        """
        return len(self.cred_data['nodeSummaries'])
    
    @property
    def nodes(self) -> List[Any]:
        """
        Returns all nodes in the graph
        """
        return [self.get_node(i) for i in range(self.total_nodes)]   
    
    @property
    def intervals(self, to_datetime=False) -> List[Any]:
        """
        Returns timestamp intervals where cred was computed
        """
        return self.cred_data['intervals']
    
    def get_dt_intervals(self) -> List[Any]:
        """
        Return intervals in datetime format
        """
        return [datetime.fromtimestamp(interval[('endTimeMs')] / 1000) for interval in self.intervals]
    
    @property
    def distributed_cred(self) -> float:
        """
        Returns total distributed cred
        """
        if self.cache['df'] is None:
            self.to_df()
        return self.cache['df'].totalCred.sum()
    
    @property
    def distributed_grain(self) -> float:
        """
        Returns total distributed grain
        """
        if self.cache['df_grain'] is None:
            self.get_grain_distribution()
        return self.cache['df_grain'].amount.sum()
    
    @property
    def accounts(self) -> pd.DataFrame:
        """
        Returns user accounts info from 'output/accounts.json' file
        """
        if self.cache['df_accounts'] is None:
            self.cache['df_accounts'] = pd.json_normalize(self.accounts_data['accounts'])
            self.cache['df_accounts']['account.balance'] = self.cache['df_accounts']['account.balance'].map(float) / 1e18
            self.cache['df_accounts']['account.paid'] = self.cache['df_accounts']['account.paid'].map(float) / 1e18
        return self.cache['df_accounts']
    
    def get_user_nodes(self) -> pd.DataFrame:
        """
        Returns user nodes in the graph
        """
        if self.cache['df'] is None:
            self.to_df()
        return self.cache['df'][self.cache['df']['address.nodeType'] == 'IDENTITY']
    
    def get_user_ranking(self) -> pd.DataFrame:
        """
        Returns the user raking by total amount of cred gained so far
        """
        if self.cache['df_rank'] is None:
#             self.cache['df_rank'] = self.get_user_nodes().sort_values('totalCred', ascending=False).reset_index(drop=True)
#             distributed_cred = self.cache['df_rank'].totalCred.sum()
#             self.cache['df_rank']['credShare'] = (self.cache['df_rank'].totalCred / distributed_cred) * 100
            df_rank_p = self.get_user_nodes()[['address.id', 'totalCred', 'credOverTime']]
            distributed_cred = df_rank_p.totalCred.sum()
            df_rank_p['credShare'] = (df_rank_p.totalCred / distributed_cred) * 100
            df_rank_p.set_index('address.id', inplace=True)
            df_acc_p = self.accounts[['account.identity.id',
                                      'account.identity.name',
                                      'account.identity.subtype',
                                      'account.active',
                                      'account.balance',
                                      'account.paid'
                                     ]]
            self.cache['df_rank'] = df_acc_p.join(df_rank_p,
                                                  on='account.identity.id',
                                                  how='inner'
                                                 ).sort_values('totalCred', ascending=False).reset_index(drop=True)
            self.cache['df_rank'].columns = ['id', 'user', 'type', 'active', 'grainBalance', 'grainPaid', 'totalCred', 'credOverTime', 'credShare']
            
        return self.cache['df_rank']
    
    def get_grain_distribution(self) -> pd.DataFrame:
        """
        Returns the history of grain distribution
        """
        if self.cache['df_grain'] is None:
            grain_history = [acc for acc in self.accounts_data['accounts'] if 'allocationHistory' in acc['account']]
            if len(grain_history) > 0:
                grain_distribution = [{'credTimestampMs': record['credTimestampMs'], 'amount': int(record['grainReceipt']['amount']) / 1e18} \
                                      for acc in grain_history for record in acc['account']['allocationHistory']]
                self.cache['df_grain'] = pd.json_normalize(grain_distribution)
                self.cache['df_grain']['credTimestampMs'] = pd.to_datetime(self.cache['df_grain']['credTimestampMs'], unit='ms')
            else:
                # zeros
                self.cache['df_grain'] = pd.DataFrame([self.get_dt_intervals(), [0.] * len(self.intervals)]).T
                self.cache['df_grain'].columns = ['credTimestampMs', 'amount']
        return self.cache['df_grain']

    def get_cred_over_time(self) -> pd.DataFrame:
        """
        Returns distributed cred summary over all intervals
        """
        if self.cache['df_cred_ot'] is None:
            if self.cache['df'] is None:
                self.to_df()
            self.cache['df_cred_ot'] = pd.DataFrame([self.get_dt_intervals(),
                                                     pd.DataFrame(self.cache['df'].credOverTime.to_list()).sum()
                                                    ]).T
            self.cache['df_cred_ot'].columns = ['credTimestampMs', 'amount']
            self.cache['df_cred_ot'].set_index('credTimestampMs', drop=True, inplace=True)
        return self.cache['df_cred_ot']
    
    def to_df(self) -> pd.DataFrame:
        """
        Retuns all nodes data as a DataFrame
        """
        if self.cache['df'] is None:
            self.cache['df'] = pd.json_normalize(self.nodes)
            self.cache['df'].timestamp = pd.to_datetime(self.cache['df'].timestamp, unit='ms')
#             distributedCred = self.df.totalCred.sum()
#             self.df['credShare']  = self.df.totalCred / distributedCred
            
        return self.cache['df']
    
    def get_cred_flow_from_graph(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Gets cred flow through nodes & edges in the cred graph.
        """
        if self.cache['df_cred_eflow'] is None:
            
            def set_plugin(label):
                for prefix, plugin in plugin_prefixes.items():
                    if label.startswith(prefix):
                        return plugin
                return 'Not Found'
            
            # PREPROCESSING
            plugin_meta = dict()
            edges = []
            nodes = []
            # edges_weights = dict()
            # nodes_weights = dict()

            for plugin in self.cred_json_data[1]['plugins'][1]:
                plugin_meta[plugin['name']] = {
                    'nodePrefix': plugin['nodePrefix'],
                    'edgePrefix': plugin['edgePrefix'],
                    'edgeTypes': [{'prefix': et['prefix'], 'weight': et['defaultWeight']} for et in plugin['edgeTypes']],
                    'nodeTypes': [{'prefix': nt['prefix'], 'weight': nt['defaultWeight']} for nt in plugin['nodeTypes']],
                }
                edges.extend([et['prefix'] for et in plugin_meta[plugin['name']]['edgeTypes']])
            #     for et in plugin_meta[plugin['name']]['edgeTypes']:
            #         edges_weights[et['prefix']] = et['weight']
                nodes.extend([nt['prefix'] for nt in plugin_meta[plugin['name']]['nodeTypes']])
            #     for nt in plugin_meta[plugin['name']]['nodeTypes']:
            #         nodes_weights[nt['prefix']] = nt['weight']


            plugin_prefixes = {plugin_meta[p_name]['nodePrefix'].replace('\x00', ''): p_name for p_name in plugin_meta}
            plugin_prefixes.update({plugin_meta[p_name]['edgePrefix'].replace('\x00', ''): p_name for p_name in plugin_meta})
            
            # EDGES
            df_ew = pd.DataFrame([self.weighted_graph['weightsJSON'][1]['edgeWeights'].keys(),
                      [v['backwards'] for v in self.weighted_graph['weightsJSON'][1]['edgeWeights'].values()],
                      [v['forwards'] for v in self.weighted_graph['weightsJSON'][1]['edgeWeights'].values()]
                     ]).T
            df_ew.columns = ['edge', 'backward', 'forward']
            
            cred_edges = dict()
            for e in edges:
                cred_edges[e.replace('\x00', '')] = [
                    df_ew[df_ew.edge.str.startswith(e)].backward.sum(),
                    df_ew[df_ew.edge.str.startswith(e)].forward.sum()
                ]

            self.cache['df_cred_eflow'] = pd.json_normalize(cred_edges).T
            self.cache['df_cred_eflow']['backward'] = self.cache['df_cred_eflow'].iloc[:,0].apply(lambda x: x[0])
            self.cache['df_cred_eflow']['forward'] = self.cache['df_cred_eflow'].iloc[:,0].apply(lambda x: x[1])
            self.cache['df_cred_eflow']['plugin'] = self.cache['df_cred_eflow'].index.map(set_plugin)
            self.cache['df_cred_eflow'].drop(columns=[0], inplace=True)
            
            # NODES
            df_nw = pd.DataFrame([self.weighted_graph['weightsJSON'][1]['nodeWeights'].keys(),
                                  self.weighted_graph['weightsJSON'][1]['nodeWeights'].values()
                                 ]).T
            df_nw.columns = ['node', 'weight']
            
            cred_nodes = dict()
            for n in nodes:
                cred_nodes[n.replace('\x00', '')]  = df_nw[df_nw.node.str.startswith(n)].weight.sum()

            self.cache['df_cred_nflow'] = pd.json_normalize(cred_nodes).T
            self.cache['df_cred_nflow'].columns = ['weight']
            self.cache['df_cred_nflow']['plugin'] = self.cache['df_cred_nflow'].index.map(set_plugin)
            
        return (self.cache['df_cred_nflow'], self.cache['df_cred_eflow'])

    def __repr__(self) -> str:
        return "<{} - ({} nodes & {} distributed CRED)>".format(self.__class__.__name__, self.total_nodes, self.distributed_cred)

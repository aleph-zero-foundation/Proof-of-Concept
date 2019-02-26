import os
import sys
import pandas as pd
from matplotlib import rcParams
rcParams['font.family'] = 'monospace'
import matplotlib.pyplot as plt
import parse



def gen_label_from_dir_name(dir_name):
    # create_f and sync_f are parsed as strings and not floats because of some strange issue with parse
    parsed = parse.parse("{N:d}_{parents:d}_{tcoin:d}_{create_f}_{sync_f}_{recv:d}_{txpu:d}_{fast_poset:d}",dir_name)
    tc = 'TC' if parsed['tcoin'] else '  '
    label = f"N={parsed['N']:<4} TX={parsed['txpu']:<4} FQ=({float(parsed['create_f']):.1f},{float(parsed['sync_f']):.2f}) {tc}"
    return label

def gen_color_mapping(labels):
    '''
    Returns a dict mapping labels to colors.
    '''
    n_labels = len(labels)
    color_map = plt.cm.get_cmap('Blues', n_labels)
    return {labels[i]: color_map(i) for i in range(n_labels)}

def gen_colors(n_colors):
    '''
    Returns a list of n_colors colors.
    '''
    color_map = plt.cm.get_cmap('Blues', n_colors)
    return [color_map(i) for i in range(n_colors)]

def get_median(num_list):
    sorted_list = sorted(num_list)
    len_list = len(sorted_list)
    return sorted_list[len_list//2]

def gen_plot(data, plot_info, file_name):
   # fig, ax = plt.subplot
    yfont = {'fontname':'monospace'}
    width = 0.7
    ax = plt.subplot()
    ax.grid(b = True, axis = 'x', linestyle = '--', color = 'black', linewidth = 0.5, alpha = 0.5)
    #ax.xaxis.grid()
    n_bars = len(data)
    data.sort(key = lambda x: x[1])
    colors = gen_colors(n_bars)
    if plot_info['order'] != 'dec':
        data.reverse()
        colors.reverse()
    bar_pos = range(len(data))
    data_series = [val for _, val in data]
    #colors = [color_mapping[label] for label, _ in data]

    bars = ax.barh(bar_pos, data_series, width, color = colors, edgecolor = 'black', linewidth = 0.5)
    labels = [p[0] for p in data]
    #plt.setp((ax,), xticks=x_series, xticklabels=xticklabels)
    ax.set_yticks(bar_pos)
    ax.set_yticklabels(labels, fontsize=6)
    plt.subplots_adjust(left=0.3)
    plt.title(plot_info['title'])
    ax.set(xlabel = plot_info['xlabel'])

    #plt.setp((ax,),xticks=x_series, xticklabels=xticklabels, fontsize=8, rotation=45)
    #ax.set_fontsize(8)
    #plt.xticks(xticks=x_series, xticklabels=xticklabels)

    #ax_sent.legend([sent_bars[0]], ['units sent'])
    #ax_recv.legend([recv_bars[0]], ['units received'])

    #ax_sent.set(xlabel='time from start (s)')
    #ax_recv.set(xlabel='time from start (s)')
    #plt.tight_layout()
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    plt.savefig(file_name, dpi=800)
    plt.close()


if len(sys.argv) < 2:
    print("Provide argument: a directory. Closing.")
    sys.exit(0)

log_dir = sys.argv[1]

list_logs = os.listdir(log_dir)
global_stats = {}
for dir_name in list_logs:
    print(dir_name)
    label = gen_label_from_dir_name(dir_name)
    stats = {}
    inner_dir = os.path.join(log_dir, dir_name, 'txt-basic')
    f_name = os.listdir(inner_dir)[0]
    a = pd.read_csv(os.path.join(inner_dir, f_name), delim_whitespace=True)
    n_stats = len(a.iloc[:])
    for i in range(n_stats):
        #print(a.iloc[i])
        stat_name = a.iloc[i]['name']
        stat_val = a.iloc[i]['avg']
        stats[stat_name] = stats.get(stat_name, [])+[stat_val]

    for name, list_vals in stats.items():
        if name not in global_stats:
            global_stats[name] = []
        global_stats[name].append((label, get_median(list_vals)))

plot_infos = {
    'n_units_decision': {'title':'Number of units ordered per timing unit',
                         'xlabel': 'Number of units',
                         'order': 'inc',
                         },
    'time_decision': {'title':'Time in sec to establish a timing unit on a level.',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'decision_height': {'title':'At which level was timing decided?',
                         'xlabel': 'Avg number of levels',
                         'order': 'inc',
                         },
    'n_txs_ordered': {'title':'Number of txs ordered per timing unit.',
                         'xlabel': 'Number of txs',
                         'order': 'dec',
                         },
    'new_level_times': {'title':'Time in sec to create a new level.',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'create_ord_del': {'title':'Latency in sec of unit validation',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'units_sent_sync': {'title':'Average number of units sent in one sync',
                         'xlabel': 'Number of units',
                         'order': 'dec',
                         },
    'units_recv_sync': {'title':'Average number of units received in one sync',
                         'xlabel': 'Number of units',
                         'order': 'dec',
                         },
    'time_per_sync': {'title':'Average duration of one sync between two processes',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'time_per_unit_ex': {'title':'Sync duration averaged by the number of units exchanged',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'bytes_per_unit_ex': {'title':'Average number of bytes exchanged per one unit',
                         'xlabel': 'Bytes',
                         'order': 'inc',
                         },
    'sync_fail': {'title':'Number of synchronization attempts that failed',
                         'xlabel': 'Number of fails',
                         'order': 'inc',
                         },
    'create_freq': {'title':'Actual average create frequency.',
                         'xlabel': 'Time [s]',
                         'order': 'dec',
                         },
    'sync_freq': {'title':'Actual average sync frequency',
                         'xlabel': 'Time [s]',
                         'order': 'dec',
                         },
    'memory_MiB': {'title':'Average memory consumption',
                         'xlabel': 'Memory [MiB]',
                         'order': 'inc',
                         },
    'time_verify' : {'title':'Average time to verify all received units per sync',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'time_add_units': {'title':'Average time to add all received units to poset per sync',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'time_cpu_sync': {'title':'Average cpu time spent on one sync',
                         'xlabel': 'Time [s]',
                         'order': 'inc',
                         },
    'n_parents' : {'title':'Average number of parents of units in the poset.',
                         'xlabel': 'Number of parents',
                         'order': 'dec',
                         },
}
for name, data in global_stats.items():
    if name in plot_infos:
        gen_plot(data, plot_infos[name], os.path.join('dsgdsg', name+'.png'))

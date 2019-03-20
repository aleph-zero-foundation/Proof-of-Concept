import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import parse


def match_dir_name(dir_name, n_processes = None, n_parents = None, create_delay = None, sync_delay = None, txpu = None):
    parsed = parse.parse("{N:d}_{parents:d}_{tcoin:d}_{create_d}_{sync_d}_{txpu:d}",dir_name)
    if parsed is None:
        return False
    if n_processes is not None and parsed['N'] != n_processes:
        return False
    if n_parents is not None and parsed['parents'] != n_parents:
        return False
    if create_delay is not None and abs(create_delay - float(parsed['create_d'])) > 1e-9:
        return False
    if sync_delay is not None and abs(sync_delay - float(parsed['sync_d'])) > 1e-9:
        return False
    if txpu is not None and parsed['txpu'] != txpu:
        return False
    return True


def get_stat_value(log_dir, target_stat_name, n_processes = None, n_parents = None, create_delay = None, sync_delay = None, txpu = None):
    list_logs = os.listdir(log_dir)
    matching_dirs = [dir_name for dir_name in list_logs if match_dir_name(dir_name, n_processes, n_parents, create_delay, sync_delay, txpu)]
    #print(list_logs)
    #print(matching_dirs)
    #print(n_processes, n_parents, create_delay, sync_delay, txpu)
    assert len(matching_dirs) == 1
    target_dir = matching_dirs[0]
    inner_dir = os.path.join(log_dir, target_dir, 'txt-basic')
    values = []
    for f_name in os.listdir(inner_dir):
        a = pd.read_csv(os.path.join(inner_dir, f_name), delim_whitespace=True)
        n_stats = len(a.iloc[:])
        for i in range(n_stats):
            stat_name = a.iloc[i]['name']
            stat_val = a.iloc[i]['avg']
            if stat_name == target_stat_name:
                values.append(stat_val)
    return get_median(values)


def get_median(num_list):
    sorted_list = sorted(num_list)
    len_list = len(sorted_list)
    return sorted_list[len_list//2]


def gen_colors(n_colors):
    '''
    Returns a list of n_colors colors.
    '''
    color_map = plt.cm.get_cmap('summer', n_colors)
    return [color_map(i) for i in range(n_colors)]


def generate_txps_vs_latency(log_dir, out_dir):
    n_proc_list = [32, 64, 128]
    sync_delay = 0.125
    n_parents = 10


    for n_processes in n_proc_list:
        create_delay = 1.0 if n_processes in [32, 64] else 2.0
        latency, txps = [], []
        for txpu in [1, 100, 1000]:
            lat_val = get_stat_value(log_dir, 'create_ord_del',
                                n_processes = n_processes,
                                n_parents = n_parents,
                                create_delay = create_delay,
                                sync_delay = sync_delay,
                                txpu = txpu)
            txps_val = get_stat_value(log_dir, 'txps',
                                n_processes = n_processes,
                                n_parents = n_parents,
                                create_delay = create_delay,
                                sync_delay = sync_delay,
                                txpu = txpu)

            latency.append(lat_val)
            txps.append(txps_val)
        plt.semilogx(txps, latency, marker='o', markersize=6, linewidth=3, label = str(n_processes))#markerfacecolor='blue', color='skyblue',

    plt.legend(title = "Committee size")
    ax = plt.gca()
    ax.set_xlim(xmax = 10**5)
    ax.set(xlabel = "transactions per second")
    ax.set(ylabel = "latency [s]")
    plt.title("Transactions per second vs Latency", fontsize = 14, fontweight = "bold")
    plt_file_path = os.path.join(out_dir, 'lat_vs_txps.png')
    plt.savefig(plt_file_path, dpi=800)
    plt.close()
    print("txps_vs_latency generated")


def generate_bars_big(log_dir, out_dir):
    n_proc_list = [128, 256, 512]
    #colors=['red', 'green', 'blue']
    colors = gen_colors(len(n_proc_list))
    fig, (ax_txps, ax_lat) = plt.subplots(1,2)
    latency, txps = [], []
    for n_processes in n_proc_list:
        txpu = 100
        lat_val = get_stat_value(log_dir, 'create_ord_del', n_processes = n_processes, txpu = txpu)
        txps_val = get_stat_value(log_dir, 'txps', n_processes = n_processes, txpu = txpu)
        latency.append(lat_val)
        txps.append(txps_val)

    width = 0.6
    for ax in [ax_txps, ax_lat]:
        ax.grid(b = True, axis = 'y', linestyle = '--', color = 'black', linewidth = 0.5, alpha = 0.3)
        ax.get_xaxis().set_ticks([])
    txps_bars = ax_txps.bar(range(3), txps, width, color = colors, edgecolor = 'black', linewidth = 1.0)
    #ax_txps.set(ylabel = "transactions per second")
    ax_txps.set_title("Transactions per sec.", size = 10)
    lat_bars = ax_lat.bar(range(3), latency, width, color = colors, edgecolor = 'black', linewidth = 1.0)
    #ax_lat.set(ylabel = "latency")
    ax_lat.set_title("Latency in sec.", size = 10)

    plt.figlegend(txps_bars, n_proc_list, loc = 'lower center', ncol = 8, labelspacing = 0.5, title = 'committee size')
    plt.suptitle("Performance of Aleph for various committee sizes", fontsize = 14, fontweight="bold")
    plt.subplots_adjust(bottom = 0.13)

    plt_file_path = os.path.join(out_dir, 'big_bars.png')
    plt.savefig(plt_file_path, dpi=800)
    plt.close()
    print("big bars generated")





def generate_plots():

    log_dir_final = sys.argv[1]
    log_dir_big = sys.argv[2]
    output_dir = sys.argv[3]

    if not os.path.isdir(output_dir):
        print(f"No such directory {output_dir}. Creating.")
        os.makedirs(output_dir, exist_ok=True)


    generate_txps_vs_latency(log_dir_final, output_dir)
    generate_bars_big(log_dir_big, output_dir)




if __name__ == '__main__':
    if len(sys.argv) != 4:
        print(
            "Usage: python generate_report_plots.py dir_final dir_big output_dir\n"
            "   Where dir_final and dir_big contain directories named using the format like: '64_10_0_1_0.15_10_1000_1'\n"
            "   Each such directory is then expected to contain a txt-basic directory generated by log_analyzer.\n"
            "   output_dir is the location where the plots should be saved."
            )
    else:
        generate_plots()
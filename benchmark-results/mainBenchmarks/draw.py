import matplotlib.pyplot as plt
import json

data = []
data_files = ['cpu2.json', 'cpu4.json','cpu8.json', 'cpu16.json']
for file in data_files:
    with open(file, 'r') as file:
        data.extend(json.load(file))

available_index = 0
available_colors = ["#b30000", "#7c1158", "#4421af", "#1a53ff", "#0d88e6", "#00b7c7", "#5ad45a", "#8be04e", "#ebdc78"]

available_markers = ['o', 's', '^', 'v', 'D', 'P', 'X', 'H']
available_line_styles = ['-', '--', '-.', ':']
color = {}
marker = {}
line_style = {}
results = {}
legend_saved = False

for dispatcher in sorted(list(set(entry['dispatcher'] for entry in data))):
    color[dispatcher] = available_colors[available_index % len(available_colors)]
    marker[dispatcher] = available_markers[available_index % len(available_markers)]
    line_style[dispatcher] = available_line_styles[available_index % len(available_line_styles)]
    available_index += 1

for entry in data:
    benchmark = entry['benchmark']
    score = entry['score']
    dispatcher = entry['dispatcher']
    num_cores = entry['num_cores']

    if benchmark not in results:
        results[benchmark] = {}

    if dispatcher not in results[benchmark]:
        results[benchmark][dispatcher] = []

    results[benchmark][dispatcher].append((num_cores, score))

for benchmark in results:
    plt.figure()

    for dispatcher in results[benchmark]:
        x, y = zip(*results[benchmark][dispatcher])
        plt.plot(x, y, marker=marker[dispatcher], linestyle=line_style[dispatcher], label=f'{dispatcher}', color=color[dispatcher])

    plt.title(f'{benchmark}')
    plt.xlabel('Number of CPU Cores')
    plt.ylabel('Score')
    plt.grid(True)
    plt.savefig(f'plots/{benchmark}.png')
    plt.legend()
    plt.savefig(f'plots/{benchmark}_with_legend.png')
    plt.close()



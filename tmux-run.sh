#! /bin/bash

tmux new -d -s DDB './build/test --config ./configs/config0.yaml -c 1'

tmux split-window -h -t DDB 'function cleanup () { killall -9 test ; } ; trap cleanup EXIT ; bash'

tmux select-pane -t 0 

tmux split-window -v -t DDB './build/test --config ./configs/config1.yaml -c 1 '

tmux select-pane -t 0 

tmux split-window -v -t DDB './build/test --config ./configs/config2.yaml -c 1 '

tmux select-pane -t 2

tmux split-window -v -t DDB './build/test --config ./configs/config3.yaml -c 1 '

tmux select-pane -t 4

tmux attach-session -t DDB

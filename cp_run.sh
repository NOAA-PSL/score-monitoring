#!/bin/sh

#cylc stop cylc8_test_soca_install2/run$1

cp scripts/map_soca_diags.py ./cylc8_test_soca_install2/bin

cylc install ./cylc8_test_soca_install2

cylc play cylc8_test_soca_install2

cylc tui

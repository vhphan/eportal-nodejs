
const {watchFolder, genericCallBack} = require("../celcomEdbWatcher");

function netanCallBack(evt, name, dir) {
    const cmd = 'cd /home/eproject/dnb && /home/eproject/anaconda3/envs/dnb/bin/python -m scripts.stats.stats_v3.daily_stats'
    genericCallBack(evt, name, dir, cmd);
}

const folderToWatch = '/home/netan3/data/AutomationForNDO';
// watchFolder(folderToWatch, netanCallBack);


function getCellId(request) {
    const {cellId: cellId1} = request.query;
    const {cellId: cellId2} = request.params;
    return cellId1 || cellId2 || request.query.object || request.params.object;
}

function getClusterId(request) {
    const {clusterId: clusterId1} = request.query;
    const {clusterId: clusterId2} = request.params;
    return clusterId1 || clusterId2 || '%';
}

module.exports = {
    getCellId,
    getClusterId
}

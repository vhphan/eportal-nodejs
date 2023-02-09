const MySQLBackend = require("../../db/MySQLBackend");
const {getCacheKeyValue} = require("../RedisBackend");
const {logger} = require("../../middleware/logger");
const {getUser} = require("../../auth");
const {getSqlResults} = require("./utils");

const mysql = new MySQLBackend('celcom');

const getProjects = async (request, response) => {
    const sqlQuery = `
                    SELECT DISTINCT Project
                    FROM eproject_cm.tblsiteinfo
                    WHERE Project IS NOT NULL and Project <> '';
                    ;
        `;
    const results = await getSqlResults(mysql, sqlQuery);
    response.status(200).json(results);
}

const getProcesses = async (request, response) => {
    const sqlQuery = `
                    SELECT DISTINCT Process
                    FROM eproject_cm.tblsiteinfo
                    WHERE Process IS NOT NULL and Process <> '';
                    ;
        `;
    const results = await getSqlResults(mysql, sqlQuery);
    response.status(200).json(results);
}

const getSimplifiedE2E = async (request, response) => {
    const projects = request.query["projects"] || '%';
    const processes = request.query["processes"] || '%';
    const sqlQuery = `
    SELECT * FROM eproject_cm.simplified_e2e
    WHERE Project IN (?)
    AND Process IN (?)
    `;
    const results = await getSqlResults(mysql, sqlQuery, [projects, processes]);
    response.status(200).json(results);
}




module.exports = {
    getProjects,
    getProcesses,
    getSimplifiedE2E,
}
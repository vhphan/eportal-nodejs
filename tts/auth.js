const ttsQueries = require('../db/pgjs/ttsQueries');
const asyncHandler = require("../middleware/async");

const checkUserPassword = asyncHandler(async (req, res, next) => {
    const { username, password } = req.body;
    const user = await ttsQueries.getUserByUsername(username, password);
    if (!user) {
        return res.status(400).json({
            success: false,
            message: "Invalid credentials"
        });
    }
    req.user = user;
    next();
});


const checkApiKey = asyncHandler(async (req, res, next) => {
    const { apiKey } = req.body;
    const user = await ttsQueries.getUserByApiKey(apiKey);
    if (!user) {
        return res.status(400).json({
            success: false,
            message: "Invalid credentials"
        });
    }
    req.user = user;
    next();
});




module.exports = {
    checkUserPassword
};
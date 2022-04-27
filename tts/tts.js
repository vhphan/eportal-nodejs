const express = require('express');
const router = express.Router();
const session = require("express-session");
const cookieParser = require("cookie-parser");

const ttsQueries = require('../db/pgjs/ttsQueries');
const asyncHandler = require("../middleware/async");

router.use(cookieParser());
router.use(session({
    secret: process.env.SESSION_SECRET,
    saveUninitialized: true,
    resave: true }));

router.post('/user', asyncHandler(ttsQueries.createUser));

router.post('/login', asyncHandler(ttsQueries.loginUser));

module.exports = router;

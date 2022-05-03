const express = require('express');
const router = express.Router();
const session = require("express-session");
const cookieParser = require("cookie-parser");

const ttsQueries = require('../db/pgjs/ttsQueries');
const asyncHandler = require("../middleware/async");
const {auth} = require("./auth");

router.use(cookieParser());
router.use(session({
    secret: process.env.SESSION_SECRET,
    saveUninitialized: true,
    resave: true }));

const unless = function(middleware, ...paths) {
  return function(req, res, next) {
    const pathCheck = paths.some(path => path === req.path);
    pathCheck ? next() : middleware(req, res, next);
  };
};

router.use(unless(auth, "/user", "/login"));

router.post('/user', asyncHandler(ttsQueries.createUser));

router.post('/login', asyncHandler(ttsQueries.loginUser));

router.get('/users', asyncHandler(ttsQueries.getUsers));

router.put('/users/:id', asyncHandler(ttsQueries.updateUser));

module.exports = router;

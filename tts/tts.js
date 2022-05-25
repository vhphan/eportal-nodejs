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

router.post('/login', asyncHandler(ttsQueries.loginUser));
router.post('/loginUserWithApiKey', asyncHandler(ttsQueries.loginUserWithApiKey));

router.post('/user', asyncHandler(ttsQueries.createUser));
router.get('/users', asyncHandler(ttsQueries.getUsers));
router.put('/users/:id', asyncHandler(ttsQueries.updateUser));
router.delete('/users/:id', asyncHandler(ttsQueries.deleteUser));

router.get('/admins', asyncHandler(ttsQueries.getAdmins));
router.get('/pms', asyncHandler(ttsQueries.getPms));
router.get('/asps', asyncHandler(ttsQueries.getAsps));

router.post('/task', asyncHandler(ttsQueries.createTask));
router.put('/task/:id', asyncHandler(ttsQueries.updateTask));
router.delete('/task/:id', asyncHandler(ttsQueries.deleteTask));

router.get('/tasks', asyncHandler(ttsQueries.getTasks));

router.get('/sendTestEmail', asyncHandler(ttsQueries.sendTestEmail));

module.exports = router;

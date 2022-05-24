
const nodemailer = require('nodemailer');

const sql = require('./PgJsBackend');
const {logger} = require("../../middleware/logger");
const {redisClient} = require("../../middleware/redisCache");
const {response} = require("express");
const testQuery = async (request, response) => {
    response.status(200).json({result: 'success'});
}

async function getUserByApiKey(apiKey) {
    const result = await sql`SELECT * FROM tts.users WHERE api_key = ${apiKey}`;
    return result[0];
}

async function getUser(email, password) {
    const results = await sql`SELECT * FROM dnb.tts.users 
    WHERE email = ${email} AND password_hash = crypt(${password}, password_hash)`;
    console.log(results);
    return results[0];
}

async function getUserWithApiKey(apiKey) {
    const results = await sql`SELECT * FROM dnb.tts.users 
    WHERE api_key = ${apiKey}`;
    console.log(results);
    return results[0];
}

async function getUserById(id) {
    const result = await sql`SELECT * FROM dnb.tts.users WHERE id = ${id}`;
    return result[0];
}


async function getUserByApi(apiKey) {
    if (!redisClient.isOpen) {
        await redisClient.connect();
    }
    const cacheResult = await redisClient.get('usersApiKey:' + apiKey);
    if (cacheResult) {
        return JSON.parse(cacheResult);
    }
    const result = await sql`SELECT * FROM dnb.tts.users WHERE api_key = ${apiKey}`;
    redisClient.set('usersApiKey:' + apiKey, JSON.stringify(result[0])).then(() => {
        console.log('set cache for usersApiKey:' + apiKey);
    }).catch((e) => {
        console.log(e);
    });
    return result[0];
}


const createStrongPassword = function () {
    var length = 10,
        charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*_|;,.<>?",
        retVal = "";
    for (var i = 0, n = charset.length; i < length; ++i) {
        retVal += charset.charAt(Math.floor(Math.random() * n));
    }
    return retVal;
}

const sendEmail = function (email, subject, message) {
    logger.info("Sending email to " + email);
    logger.info(email);
    logger.info(message);
    const transporter = nodemailer.createTransport({
        host: 'mail.eprojecttrackers.com',
        port: 465,
        auth: {
            user: 'eri_portal@eprojecttrackers.com',
            pass: process.env.MAIL_PASSWORD
        }
    });
    const mailOptions = {
        from: 'tts manager',
        to: email,
        subject: subject,
        text: message
    }
    transporter.sendMail(mailOptions, function (error, info) {
        if (error) {
            console.log(error);
            logger.error(error.message);
        } else {
            console.log('Email sent: ' + info.response);
        }
    });
}

const createUser = async (request, response) => {

    const {email, firstName, lastName, userType, remarks} = request.body;

    const password = createStrongPassword();

    const userResult = await sql`INSERT INTO dnb.tts.users (email, password_hash, first_name, last_name, user_type, remarks)
                                 VALUES (${email}, crypt(${password}, gen_salt('bf')), ${firstName}, ${lastName},
                                         ${remarks}
                                         ${userType}) returning *;`;

    const user = userResult[0];

    const tableReference = {
        'admin': 'dnb.tts.admins',
        'pm': 'dnb.tts.pm',
        'dt': 'dnb.tts.dt',
    }

    const userTypeTable = tableReference[userType];

    const userTypeResult = await sql`INSERT INTO ${sql(userTypeTable)} (user_id)
                                     VALUES (${user.id}) returning *;
    `;

    if (!!userTypeResult[0].id) {
        sendEmail(email, password);
    }
    response.status(200).json({success: !!userTypeResult[0].id});
}

async function loginUser(request, response) {
    const {email, password} = request.body;
    const user = await getUser(email, password);
    if (!!user) {
        request.session.user = user;
        request.session.save();

        response.cookie(
            'api', user['api_key'],
            {maxAge: 900000, httpOnly: true});

        response.status(200).json({
            success: true,
            message: 'Login successful!',
            active: user.active,
            apiKey: user['api_key'],
            user: user
        });
    } else {
        response.status(403).json({success: false, message: 'Invalid email or password'});
    }
}

async function loginUserWithApiKey(request, response) {
    const {apiKey} = request.body;
    const user = await getUserWithApiKey(apiKey);
    if (!!user) {
        request.session.user = user;
        request.session.save();

        response.cookie(
            'api', user['api_key'],
            {maxAge: 900000, httpOnly: true});

        response.status(200).json({
            success: true,
            message: 'Login successful!',
            active: user.active,
            apiKey: user['api_key'],
            user: user
        });
    } else {
        response.status(403).json({success: false, message: 'Invalid api key'});
    }
}

function logoutUser(request, response) {
    request.session.destroy();
    response.status(200).json({success: true, message: 'Logged out'});
}

async function getUsers(request, response) {
    const results = await sql`
    SELECT users.id,
       first_name,
       last_name,
       email,
       created_at,
       user_type,
       remarks,
       pm.admin_id,
       dt.pm_id,
       active,
       CASE WHEN user_type = 'pm' THEN pm.asp_id 
            WHEN user_type = 'dt' THEN dt.asp_id
            ELSE NULL
       END AS asp_id
FROM dnb.tts.users
         LEFT JOIN dnb.tts.admins
                   ON dnb.tts.users.id = dnb.tts.admins.user_id
         LEFT JOIN dnb.tts.pm ON dnb.tts.users.id = dnb.tts.pm.user_id
         LEFT JOIN dnb.tts.dt ON dnb.tts.users.id = dnb.tts.dt.user_id
;`;
    response.status(200).json({success: true, data: results, message: 'Users retrieved'});
}

async function preCheckUserRights(request, response) {
    const id = request.params.id;
    const requestor = await getUserByApi(request.headers.api);
    const requestorType = requestor.user_type;
    const existingUserData = await getUserById(id);
    const existingUserType = existingUserData.user_type;

    if (requestorType !== 'admin' && existingUserType === 'admin') {
        response.status(403).json({success: false, message: 'Insufficient user rights to perform this action'});
    }
    if (requestorType === 'dt' && existingUserType === 'pm') {
        response.status(403).json({success: false, message: 'Insufficient user rights to perform this action'});
    }
    if (requestorType === 'dt' && existingUserType === 'dt') {
        response.status(403).json({success: false, message: 'Insufficient user rights to perform this action'});
    }
    return id;
}

async function deleteUser(request, response) {
    const id = preCheckUserRights(request, response);
    const user = await sql`DELETE FROM dnb.tts.users
                           WHERE id = ${id} returning *;`;
    response.status(200).json({success: true, message: 'User deleted successfully!', data: user});
}

async function updateUser(request, response) {
    const id = await preCheckUserRights(request, response);
    const {
        email,
        first_name: firstName,
        last_name: lastName,
        user_type: userType,
        remarks,
        admin_id: adminId,
        pm_id: pmId,
        asp_id: aspId,
        active
    } = request.body;
    const user = await sql`UPDATE dnb.tts.users
                           SET email      = ${email},
                               first_name = ${firstName},
                               last_name  = ${lastName},
                               user_type  = ${userType},
                               remarks    = ${remarks},
                               active     = ${active === 'true'}
                           WHERE id = ${id} returning *;`;
    response.status(200).json({success: true, message: 'User updated successfully!', data: user});
}

async function preChecksUserRightsForTask(request, response) {
    const requestor = await getUserByApi(request.headers.api);
    const requestorType = requestor.user_type;
    const requestorId = requestor.id;
    if (requestorType === 'dt') {
        response.status(403).json({success: false, message: 'Insufficient user rights to perform this action'});
    }
    return requestorId;
}

async function createTask(request, response) {
    const requestorId = await preChecksUserRightsForTask(request, response);
    const {
        taskName,
        taskType,
        taskDescription,
        taskPlanStartDate,
        taskPlanEndDate,
    } = request.body;
    const task = await sql`INSERT INTO dnb.tts.tasks (task_name,
                                                      task_type,
                                                      task_description,
                                                      task_plan_start_date,
                                                      task_plan_end_date,
                                                      created_by)
                           VALUES (${taskName},
                                   ${taskType},
                                   ${taskDescription},
                                   ${taskPlanStartDate},
                                   ${taskPlanEndDate},
                                   ${requestorId}) returning *;`;

    response.status(200).json({success: true, message: 'Task created successfully!', data: task});
}

async function deleteTask(request, response) {
    await preChecksUserRightsForTask(request, response);
    const id = request.params.id;
    const task = await sql`DELETE FROM dnb.tts.tasks
                           WHERE id = ${id} returning *;`;
    response.status(200).json({success: true, message: 'Task deleted successfully!', data: task});
}

async function updateTask(request, response){
    const requestorId = await preChecksUserRightsForTask(request, response);
    const {
        taskName,
        taskType,
        taskDescription,
        taskPlanStartDate,
        taskPlanEndDate,
    } = request.body;
    const id = request.params.id;
    const task = await sql`UPDATE dnb.tts.tasks
                           SET task_name        = ${taskName},
                               task_type        = ${taskType},
                               task_description = ${taskDescription},
                               task_plan_start_date = ${taskPlanStartDate},
                               task_plan_end_date   = ${taskPlanEndDate},
                               updated_by       = ${requestorId}
                           WHERE id = ${id} returning *;`;
    response.status(200).json({success: true, message: 'Task updated successfully!', data: task});
}

async function getTasks(request, response) {
    const tasks = await sql`SELECT 
    * 
    FROM dnb.tts.tasks order by id desc;`;
    response.status(200).json({success: true, message: 'Tasks fetched successfully!', data: tasks});
}

async function sendTestEmail(request, response) {
    sendEmail('vee.huen.phan@ericsson.com', 'Test email', 'This is a test email');
    response.status(200).json({success: true, message: 'Email send job triggered!'});
}


module.exports = {
    sendTestEmail,
    deleteUser,
    testQuery,
    createUser,
    getUser,
    getUserByApiKey,
    loginUser,
    logoutUser,
    getUsers,
    updateUser,
    createTask,
    getTasks,
    loginUserWithApiKey,
    deleteTask,
    updateTask
};
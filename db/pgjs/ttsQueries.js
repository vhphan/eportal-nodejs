
const nodemailer = require('nodemailer');

const sql = require('./PgJsBackend');
const {logger} = require("../../middleware/logger");
const testQuery = async (request, response) => {
    response.status(200).json({result: 'success'});
}

async function getUserByApiKey(apiKey) {

}

async function getUser(email, password) {

    const results = await sql`SELECT * FROM dnb.tts.users ` +
        `WHERE email = ${email} AND password = crypt(${password}, gen_salt('bf'))`;

    return results[0];

}

const createStrongPassword = function () {
    var length = 10,
        charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-=[]{}|;':\",./<>?",
        retVal = "";
    for (var i = 0, n = charset.length; i < length; ++i) {
        retVal += charset.charAt(Math.floor(Math.random() * n));
    }
    return retVal;
}

const sendEmail = function (email, password) {

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
        subject: 'TTS Portal Password',
        text: 'Your password is ' + password + '.'
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

    const results1 = await sql`INSERT INTO dnb.tts.users (email, password, first_name, last_name, user_type)
                               VALUES (${email}, crypt(${password}, gen_salt('bf')), ${firstName}, ${lastName},
                                       ${userType}) returning ID;`;

    const userID = results1[0].id;

    const tableReference = {
        'admin': 'dnb.tts.admins',
        'pm': 'dnb.tts.pm',
        'dt': 'dnb.tts.dt',
    }
    const userTypeTable = tableReference[userType];

    const results2 = await sql`INSERT INTO ${sql(userTypeTable)} (user_id, remarks)
                               VALUES (${userID}, ${remarks}) returning id;
    `;

    if (!!results2[0].id) {
        sendEmail(email, password);
    }

    response.status(200).json({success: !!results2[0].id});

}

export function loginUser(request, response) {
    const {email, password} = request.body;
    const user = getUser(email, password);
    if (!!user) {
        request.session.user = user;
        request.session.save();
        response.status(200).json({success: true, message: 'Login successful'});
    } else {
        response.status(200).json({success: false});
    }
}

export function logoutUser(request, response) {
    request.session.destroy();
    response.status(200).json({success: true, message: 'Logged out'});
}

module.exports = {

    testQuery,
    createUser,
    getUserByUsername: getUser,
    getUserByApiKey,
    loginUser,
    logoutUser

};
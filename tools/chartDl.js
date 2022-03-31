const echarts = require('echarts');

function sampleChart() {
// In SSR mode the first container parameter is not required
    const chart = echarts.init(null, null, {
        renderer: 'svg', // must use SVG rendering mode
        ssr: true, // enable SSR
        width: 400, // need to specify height and width
        height: 300
    });

// use setOption as normal
    chart.setOption({
        xAxis: {
            type: "category",
            data: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        },
        yAxis: {
            type: "value"
        },
        series: [
            {
                data: [120, 200, 150, 80, 70, 110, 130],
                type: "bar"
            }
        ]
    });
    return chart;
}

const getSampleChart = function () {
    return async function (req, res) {

        const chart = sampleChart();

// Output a string
        res.setHeader('Content-disposition', 'attachment; filename=sample.svg');
        res.set('Content-Type', 'application/svg+xml');
        let s1 = chart.renderToSVGString();
        let s2 = chart.renderToSVGString();
        let s = s1 + s2;
        res.send(new Buffer('<body>' + s + '</body>'));

    };
}

// const getSampleChartAsPng = function () {
//     return async function (req, res) {
//         const chart = sampleChart();
//         let svg = chart.renderToSVGString();
//         res.set('Content-Type', 'image/png');
//         res.set('Content-disposition', 'attachment; filename=sample.png');
//         let outputBuffer = await svg2png({
//             input: svg,
//             encoding: 'buffer',
//             format: 'png',
//         })
//         res.send(new Buffer(outputBuffer));
//     }
// }


module.exports = {
    getSampleChart,
};
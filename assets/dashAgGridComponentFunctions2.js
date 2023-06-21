var dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {};


// use for making dbc.Progress
dagcomponentfuncs.DBC_Progress2 = function (props) {
    const { setData, data } = props;

    function onClick() {
        setData(props.value);
    }
    return React.createElement(
        window.dash_bootstrap_components.Progress,
        {
            onClick,
            animated: props.animated,
            className: props.className,
            color: props.color,
            label: '',
            max: props.max,
            min: props.min,
            striped: props.striped,
            style: props.style,
            value: props.value
        },
    );
};
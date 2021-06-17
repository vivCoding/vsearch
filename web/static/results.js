$(() => {
    $(".pageResult").click(function () {
        window.location = $(this).find("a:first").attr("href");
        return false;
    });

    $(".pageResult").hover(
        function () {
            window.status = $(this).find("a:first").attr("href");
        },
        function () {
            window.status = "";
        }
    );
})
use indicatif::{ProgressBar, ProgressStyle};

pub fn create_spinner(message: String) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();

    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&["-", "\\", "|", "/"])
            .template("{spinner:.green} {msg}"),
    );

    spinner.set_message(message);
    spinner.enable_steady_tick(100);
    spinner
}

pub fn create_bar(len: u64) -> ProgressBar {
    let pb = ProgressBar::new(len);

    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .progress_chars("#>-"),
    );

    pb
}

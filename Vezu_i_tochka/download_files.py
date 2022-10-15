import ftplib
import os
import sys
import time


def download_files(destination: str):
    ftp = ftplib.FTP_TLS('de-edu-db.chronosavant.ru')
    ftp.login('etl_tech_user', 'etl_tech_user_password')
    ftp.prot_p()
    ftp.retrlines('LIST')

    def download_folder(folder: str, final_destination: str):
        try:
            ftp.cwd(folder)
            os.chdir(final_destination)
            mkdir_p(final_destination[0:len(final_destination) - 1] + folder)
            print("Created: " + final_destination[0:len(final_destination) - 1] + folder)
        except OSError:
            pass
        except ftplib.error_perm:
            print("Error: could not change to " + folder, sys.exit("Ending Application"))

        filelist = ftp.nlst()

        for file in filelist:
            time.sleep(0.05)
            try:
                ftp.cwd(folder + file + "/")
                download_folder(folder + file + "/", final_destination)
            except ftplib.error_perm:
                os.chdir(final_destination[0:len(final_destination) - 1] + folder)

                try:
                    ftp.retrbinary("RETR " + file, open(os.path.join(final_destination + folder, file), "wb").write)
                    print("Downloaded: " + file)
                except Exception:
                    print("Error: File could not be downloaded " + file)
        return

    def mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if os.path.isdir(path):
                pass
            else:
                raise

    download_folder('/payments', destination)
    download_folder('/waybills', destination)


if __name__ == '__main__':
    download_files('c:/Users/maxim/Downloads/Hackaton/Files/')

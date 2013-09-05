#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <dirent.h>
#include <sys/wait.h>

#include "file_directory_utils.h"
#include "blade_common_define.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace common
{
/** 
 * @brief remove any ending forward slashes from directory paths 
 * 
 * @param fname file name(modifies argument)
 * 
 * @return file name 
 */

static char *strip_tail_dir_slashes (char *fname, const int32_t len)
{
	if (fname == NULL)
		return NULL;

	int32_t i = len - 1;
	if (len > 1)
	{
		while (fname[i] == '/')
		{
			fname[i--] = '\0';
		}
	}
	return fname;
}

//return true if filename is exists
bool FileDirectoryUtils::exists (const char *filename)
{
	if (filename == NULL)
		return false;

	int32_t iLen = strlen(filename);
	if (iLen > MAX_PATH || iLen <= 0)
		return false;

	struct stat64 file_info;
	char path[MAX_PATH + 1];
	strncpy(path, filename, iLen);
	path[iLen] = '\0';
	char *copy = strip_tail_dir_slashes (path, iLen);
	int32_t result = stat64 (copy, &file_info);

	return (result == 0);
}

//return ture if dirname is a directory
bool FileDirectoryUtils::is_directory (const char *dirname)
{
	if (dirname == NULL)
		return false;

	int32_t iLen = strlen(dirname);
	if (iLen > MAX_PATH || iLen <= 0)
		return false;

	struct stat64 file_info;
	char path[MAX_PATH + 1];
	strncpy(path, dirname, iLen);
	path[iLen] = '\0';
	char *copy = strip_tail_dir_slashes (path, iLen);
	int32_t result = stat64 (copy, &file_info);

	return result < 0 ? false : S_ISDIR (file_info.st_mode);
}

//create the give dirname, return true on success or dirname exists 
bool FileDirectoryUtils::create_directory (const char *dirname)
{
	if (dirname == NULL)
		return false;

	mode_t umake_value = umask (0);
	umask (umake_value);
	mode_t mode = (S_IRWXUGO & (~umake_value)) | S_IWUSR | S_IXUSR;
	bool bRet = false;
	if (mkdir(dirname, mode) == 0)
		bRet = true;

	if (!bRet)
	{
		if (errno == EEXIST)
			bRet = true;
	}
	return bRet;
}

//creates the full path of fullpath, return true on success
bool FileDirectoryUtils::create_full_path (const char *fullpath)
{
	if (fullpath == NULL)
		return false;

	int32_t iLen = strlen(fullpath);
	if (iLen > MAX_PATH || iLen <= 0x01)
		return false;

	bool bRet = true;
	struct stat stats;
	if (lstat (fullpath, &stats) == 0 && S_ISDIR (stats.st_mode))
		bRet = false;

	char dirpath[MAX_PATH + 1];
	strncpy(dirpath, fullpath, iLen);
	dirpath[iLen] = '\0';

	char *path = dirpath;
	if (bRet)
	{
		while (*path == '/')
			path++;
	}
	while (bRet)
	{
		path = strchr (path, '/');
		if (path == NULL)
		{
			break;
		}

		*path = '\0';

		if (!is_directory (dirpath))
		{
			if (!create_directory (dirpath))
			{
				bRet = false;
				break;
			}
		}
		*path++ = '/';

		while (*path == '/')
		{
			path++;
		}
	}

	if (bRet)
	{
		if (!is_directory(dirpath))
		{
			if (!create_directory(dirpath))
				bRet = false;
		}
	}
	return bRet;
}

//delete the given file, return true if filename exists
//return true on success
bool FileDirectoryUtils::delete_file (const char *filename)
{
	if (filename == NULL)
		return false;

	bool bRet = true;
	bool bExists = true;

	if (!exists (filename))
		bExists = false;

	if (bExists)
	{
		if (is_directory(filename))
			bRet = false;
	}

	if (bRet && bExists)
	{
		if (unlink (filename) == -1)
			bRet = false;
	}
	return !bExists ? true : bRet; 
}

//delete the given directory and anything under it. Returns true on success
bool FileDirectoryUtils::delete_directory (const char *dirname)
{
	if (dirname == NULL)
		return false;

	bool bRet = true;
	bool bExists = true;
	if (!exists(dirname))
		bExists = false;

	if (bExists)
	{
		if (!is_directory (dirname))
			bRet = false;
	}

	if (bRet && bExists)
	{
		if (rmdir (dirname) != 0)
			bRet = false; 
	}
	return !bExists ? true : bRet;
}

//delete the given directory and anything under it. Returns true on success
bool FileDirectoryUtils::delete_directory_recursively (const char *directory)
{
	if (directory == NULL)
		return false;

	struct dirent dirent;
	struct dirent *result = NULL;
	DIR *dir = NULL;
	bool bRet = true; 
	dir = opendir (directory);
	if (!dir)
	{
		bRet = false;
	}

	while (!readdir_r (dir, &dirent, &result) && result && bRet)
	{
		char *name = result->d_name;
		if (((name[0] == '.') && (name[1] == '\0')) || ((name[0] == '.') && (name[1] == '.') && (name[2] == '\0')))
		{
			continue;
		}

		char path[MAX_PATH];
		snprintf (path, MAX_PATH, "%s%c%s", directory, '/', name);
		if (is_directory (path))
		{
			if (!delete_directory_recursively (path))
			{
				bRet = false;
				break;
			}
		}
		else
		{
			if (!delete_file (path))
			{
				bRet = false;
				break;
			}
		}
	}

	if (dir != NULL)
		closedir(dir);

	return bRet ? delete_directory(directory) : bRet;
}

bool FileDirectoryUtils::rename (const char *srcfilename, const char *destfilename)
{
	if (srcfilename == NULL || destfilename == NULL)
		return false;

	bool bRet = true;
	if (::rename (srcfilename, destfilename) != 0)
		bRet = false;
	return bRet;
}

//return the size of filename 
int64_t FileDirectoryUtils::get_size (const char *filename)
{
	if (filename == NULL)
		return 0;

	int64_t iRet = 0;
	bool bExists = true;
	if (!exists (filename))
		bExists = false;

	if (bExists)
	{
		if (is_directory (filename))
			bExists = false;  
	}

	if (bExists)
	{
		struct stat64 file_info;
		if (stat64 (filename, &file_info) == 0)
			iRet = file_info.st_size;
	}
	return iRet;
}

int FileDirectoryUtils::vsystem(const char* cmd)
{
	int ret = 0;
	int err = 0;
	int status = 0;

	int pid = vfork();
	if (0 == pid)
	{
		err = execlp("sh", "sh", "-c", cmd, (char*)NULL);
		if (-1 == err)
		{
			LOGV(LL_ERROR, "execlp error: %s", strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			ret = BLADE_ERROR;
			LOGV(LL_ERROR, "program should not execute behind the \'execl\'");
		}
	}
	else if (-1 == pid)
	{
		LOGV(LL_ERROR, "vfork error: %s", strerror(errno));
		ret = BLADE_ERROR;
	}
	else
	{
		err = waitpid(pid, &status, 0);
		if (-1 == err)
		{
			LOGV(LL_ERROR, "waitpid error: %s", strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			ret = WEXITSTATUS(status);
		}
	}

	return ret;
}

int FileDirectoryUtils::cp(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name)
{
	int ret = BLADE_SUCCESS;

	int err = 0;

	if (NULL == src_name || NULL == src_path || NULL == dst_name || NULL == dst_path)
	{
		LOGV(LL_WARN, "Arguments are invalid, src_name=%s src_path=%s dst_naem=%s dst_path=%s", src_name, src_path, dst_name, dst_path);
		ret = BLADE_INVALID_ARGUMENT;
	}

	const char* CP_CMD_FORMAT1 = "cp %s/%s %s/%s";
	const char* CP_CMD_FORMAT2 = "cp %s %s/%s";
	const char* CP_CMD_FORMAT3 = "cp %s/%s %s";
	const char* CP_CMD_FORMAT4 = "cp %s %s";
	char *cmd = NULL;
	int cp_cmd_len = strlen(src_name) + strlen(dst_name) + strlen(src_path) + strlen(dst_path) + strlen(CP_CMD_FORMAT1);

	if (BLADE_SUCCESS == ret)
	{
		cmd = static_cast<char*>(malloc(cp_cmd_len));
		if (NULL == cmd)
		{
			LOGV(LL_WARN, "malloc error, cp_cmd_len=%ld", cp_cmd_len);
			ret = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		if (strlen(src_path) > 0)
		{
			if (strlen(dst_path) > 0)
			{
				err = snprintf(cmd, cp_cmd_len, CP_CMD_FORMAT1, src_path, src_name, dst_path, dst_name);
			}
			else
			{
				err = snprintf(cmd, cp_cmd_len, CP_CMD_FORMAT3, src_path, src_name, dst_name);
			}
		}
		else
		{
			if (strlen(dst_path) > 0)
			{
				err = snprintf(cmd, cp_cmd_len, CP_CMD_FORMAT2, src_name, dst_path, dst_name);
			}
			else
			{
				err = snprintf(cmd, cp_cmd_len, CP_CMD_FORMAT4, src_name, dst_name);
			}
		}

		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf cmd[cp_cmd_len=%ld err=%d] error[%s]", cp_cmd_len, err, strerror(errno));
			ret = BLADE_ERROR;
		}
		else if (err >= cp_cmd_len)
		{
			LOGV(LL_ERROR, "cmd buffer is not enough[cp_cmd_len=%ld err=%d]", cp_cmd_len, err);
			ret = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		err = vsystem(cmd);
		if (0 != err)
		{
			LOGV(LL_ERROR, "cp[\"%s\"] error[err=%d]", cmd, err);
			ret = BLADE_ERROR;
		}
	}

	if (NULL != cmd)
	{
		free(cmd);
		cmd = NULL;
	}

	return ret;
}

int FileDirectoryUtils::cp_safe(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name)
{
	int ret = BLADE_SUCCESS;

	if (NULL == src_name || NULL == dst_name || NULL == src_path || NULL == dst_path)
	{
		LOGV(LL_WARN, "Arguments are invalid, src_name=%s dst_name=%s src_path=%s dst_path=%s", src_name, dst_name, src_path, dst_path);
		ret = BLADE_INVALID_ARGUMENT;
	}

	const char* tmp_postfix = "_XXXXXX";
	char *tmp_name = NULL;

	if (BLADE_SUCCESS == ret)
	{
		int64_t tmp_name_len = strlen(dst_path) + strlen(dst_name) + strlen(tmp_postfix) + 2;
		tmp_name = static_cast<char*>(malloc(tmp_name_len));
		if (NULL == tmp_name)
		{
			LOGV(LL_ERROR, "malloc error, tmp_name_len=%ld", tmp_name_len);
			ret = BLADE_ERROR;
		}
		else
		{
			tmp_name[0] = '\0';
			if (strlen(dst_path) > 0)
			{
				strcpy(tmp_name, dst_path);
				strcat(tmp_name, "/");
			}
			strcat(tmp_name, dst_name);
			strcat(tmp_name, tmp_postfix);
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		int fd = mkstemp(tmp_name);
		if (-1 == fd)
		{
			LOGV(LL_ERROR, "mkstemp error: %s", strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			int err = close(fd);
			if (-1 == err)
			{
				LOGV(LL_ERROR, "close file error: %s", strerror(errno));
				ret = BLADE_ERROR;
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		ret = cp(src_path, src_name, "", tmp_name);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "cp error, ret=%d", ret);
		}
		else
		{
			ret = mv("", tmp_name, dst_path, dst_name);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "mv error, ret=%d", ret);
				rm(dst_path, tmp_name);
			}
		}
	}

	if (NULL != tmp_name)
	{
		free(tmp_name);
		tmp_name = NULL;
	}

	return ret;
}

int FileDirectoryUtils::mv(const char* src_path, const char* src_name, const char* dst_path, const char* dst_name)
{
	int ret = BLADE_SUCCESS;

	int err = 0;

	if (NULL == src_path || NULL == dst_path || NULL == src_name || NULL == dst_name)
	{
		LOGV(LL_WARN, "Arguments are invalid, src_path=%s dst_path=%s src_name=%s dst_name=%s", src_path, dst_path, src_name, dst_name);
		ret = BLADE_INVALID_ARGUMENT;
	}

	const char* MV_CMD_FORMAT1 = "mv %s/%s %s/%s";
	const char* MV_CMD_FORMAT2 = "mv %s %s/%s";
	const char* MV_CMD_FORMAT3 = "mv %s/%s %s";
	const char* MV_CMD_FORMAT4 = "mv %s %s";
	char *cmd = NULL;
	int mv_cmd_len = strlen(src_path) + strlen(dst_path) + strlen(src_name) + strlen(dst_name) + strlen(MV_CMD_FORMAT1);

	if (BLADE_SUCCESS == ret)
	{
		cmd = static_cast<char*>(malloc(mv_cmd_len));
		if (NULL == cmd)
		{
			LOGV(LL_WARN, "malloc error, mv_cmd_len=%ld", mv_cmd_len);
			ret = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		if (strlen(src_path) > 0)
		{
			if (strlen(dst_path) > 0)
			{
				err = snprintf(cmd, mv_cmd_len, MV_CMD_FORMAT1, src_path, src_name, dst_path, dst_name);
			}
			else
			{
				err = snprintf(cmd, mv_cmd_len, MV_CMD_FORMAT3, src_path, src_name, dst_name);
			}
		}
		else
		{
			if (strlen(dst_path) > 0)
			{
				err = snprintf(cmd, mv_cmd_len, MV_CMD_FORMAT2, src_name, dst_path, dst_name);
			}
			else
			{
				err = snprintf(cmd, mv_cmd_len, MV_CMD_FORMAT4, src_name, dst_name);
			}
		}

		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf cmd[mv_cmd_len=%ld err=%d] error[%s]", mv_cmd_len, err, strerror(errno));
			ret = BLADE_ERROR;
		}
		else if (err >= mv_cmd_len)
		{
			LOGV(LL_ERROR, "cmd buffer is not enough[mv_cmd_len=%ld err=%d]", mv_cmd_len, err);
			ret = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		err = vsystem(cmd);
		if (0 != err)
		{
			LOGV(LL_ERROR, "mv[\"%s\"] error[err=%d]", cmd, err);
			ret = BLADE_ERROR;
		}
	}

	if (NULL != cmd)
	{
		free(cmd);
		cmd = NULL;
	}

	return ret;
}

int FileDirectoryUtils::rm(const char* path, const char* name)
{
	int ret = BLADE_SUCCESS;

	if (NULL == name || NULL == path)
	{
		LOGV(LL_WARN, "Arguments are invalid, path=%s name=%s", path, name);
		ret = BLADE_INVALID_ARGUMENT;
	}

	int full_name_len = strlen(name) + strlen(path) + 2;
	char* full_name = NULL;

	if (BLADE_SUCCESS == ret)
	{
		full_name = static_cast<char*>(malloc(full_name_len));
		if (NULL == full_name)
		{
			LOGV(LL_WARN, "malloc error, full_name_len=%ld", full_name_len);
			ret = BLADE_ERROR;
		}
		else
		{
			full_name[0] = '\0';
			if (strlen(path) > 0)
			{
				strcpy(full_name, path);
				strcat(full_name, "/");
			}
			strcat(full_name, name);
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		int err = unlink(full_name);
		if (0 != err)
		{
			LOGV(LL_WARN, "unlink error, err=%d(%s)", err, strerror(errno));
			ret = BLADE_ERROR;
		}
	}

	if (NULL != full_name)
	{
		free(full_name);
		full_name = NULL;
	}

	if (BLADE_SUCCESS != ret)
	{
		LOGV(LL_WARN, "rm_ ret=%d name=%s path=%s", ret, name, path);
	}

	return ret;
}

}//end of namespace common
}//end of namespace bladestore

